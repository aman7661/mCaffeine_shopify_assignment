import pandas as pd
import requests
import time
import json
import sys
import os
import base64
from pathlib import Path

from credentials import *


SHOP_URL = CONFIGURATION_SHOP_URL
ADMIN_API_TOKEN = CONFIGURATION_ADMIN_API_TOKEN 
EXCEL_FILE = CONFIGURATION_EXCEL_FILE
API_VERSION = CONFIGURATION_API_VERSION
# ---------------------

BASE_URL = f"https://{SHOP_URL}/admin/api/{API_VERSION}/graphql.json"
HEADERS = {
    "X-Shopify-Access-Token": ADMIN_API_TOKEN,
    "Content-Type": "application/json"
}

def run_graphql_query(query, variables):
    """
    Runs a GraphQL query/mutation with rate limiting.
    """
    try:
        response = requests.post(BASE_URL, headers=HEADERS, json={'query': query, 'variables': variables})
        response.raise_for_status()
        
        response_data = response.json()
        

        if 'errors' in response_data:
            return None

        if 'extensions' in response_data and 'cost' in response_data['extensions']:
            cost = response_data['extensions']['cost']
            available = cost.get('throttleStatus', {}).get('currentlyAvailable', 1000)
            if available < 200:
                print("Rate limit low. Sleeping for 5 seconds...")
                time.sleep(5)
        
        return response_data.get('data')

    except requests.exceptions.RequestException as e:
        print(f"HTTP Request Error: {e}", file=sys.stderr)
        return None
    except json.JSONDecodeError:
        print(f"Error decoding JSON response: {response.text}", file=sys.stderr)
        return None

def upload_image_to_shopify(image_path):
    """
    Uploads an image file to Shopify and returns the image ID.
    Supports local file paths and URLs.
    """
    if not image_path or pd.isna(image_path):
        return None
    
    image_path = str(image_path).strip()
    
    if image_path.startswith('http://') or image_path.startswith('https://'):
        return image_path
    
    if os.path.exists(image_path):
        try:
            mutation = """
            mutation fileCreate($files: [FileCreateInput!]!) {
              fileCreate(files: $files) {
                files {
                  id
                  fileStatus
                  ... on MediaImage {
                    image {
                      id
                      url
                    }
                  }
                }
                userErrors {
                  field
                  message
                }
              }
            }
            """
            
            with open(image_path, 'rb') as f:
                file_content = f.read()
                file_base64 = base64.b64encode(file_content).decode('utf-8')
            
            file_ext = Path(image_path).suffix.lower()
            mime_type_map = {
                '.jpg': 'image/jpeg',
                '.jpeg': 'image/jpeg',
                '.png': 'image/png',
                '.gif': 'image/gif',
                '.webp': 'image/webp'
            }
            mime_type = mime_type_map.get(file_ext, 'image/jpeg')
            
            variables = {
                "files": [{
                    "originalSource": f"data:{mime_type};base64,{file_base64}",
                    "filename": Path(image_path).name
                }]
            }
            
            data = run_graphql_query(mutation, variables)
            if data and data.get('fileCreate'):
                if data['fileCreate'].get('userErrors'):
                    return None
                files = data['fileCreate'].get('files', [])
                if files and files[0].get('image'):
                    image_id = files[0]['image']['id']
                    print(f"Successfully uploaded image: {image_path}")
                    return image_id
        except Exception as e:
            return None
    else:
        return None
    
    return None

def add_images_to_product(product_id, image_paths):
    """
    Adds images to a product. image_paths can be a comma-separated string or list.
    """
    if not image_paths or pd.isna(image_paths):
        return
    
    if isinstance(image_paths, str):
        paths = [p.strip() for p in image_paths.split(',') if p.strip()]
    else:
        paths = [str(image_paths)]
    
    if not paths:
        return
    
    print(f"Adding {len(paths)} image(s) to product...")
    
    image_ids = []
    for path in paths:
        image_id = upload_image_to_shopify(path)
        if image_id:
            image_ids.append(image_id)
    
    if not image_ids:
        return
    
    mutation = """
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
          images(first: 10) {
            edges {
              node {
                id
                url
              }
            }
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    variables = {
        "input": {
            "id": product_id,
            "images": image_ids
        }
    }
    
    data = run_graphql_query(mutation, variables)
    if data and data.get('productUpdate'):
      print(f"Successfully added {len(image_ids)} image(s) to product")

def create_metafield(product_id, namespace, key, value, type_name="single_line_text_field"):
    """
    Creates or updates a metafield for a product.
    """
    mutation = """
    mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields {
          id
          namespace
          key
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    variables = {
        "metafields": [{
            "ownerId": product_id,
            "namespace": namespace,
            "key": key,
            "value": str(value),
            "type": type_name
        }]
    }
    
    data = run_graphql_query(mutation, variables)
    if data and data.get('metafieldsSet'):
        if data['metafieldsSet'].get('userErrors'):
            return False
        else:
            print(f"Successfully created metafield: {namespace}.{key}")
            return True
    return False

def add_metafields_to_product(product_id, row_data):
    """
    Adds metafields to a product from Excel row data.
    Looks for columns starting with 'metafield_'
    Format: metafield_namespace_key_type (e.g., metafield_custom_brand_single_line_text_field)
    """
    metafield_columns = [col for col in row_data.index if col.startswith('metafield_')]
    
    for col in metafield_columns:
        value = row_data[col]
        if pd.isna(value) or not str(value).strip():
            continue
        
        parts = col.replace('metafield_', '').split('_', 2)
        if len(parts) >= 2:
            namespace = parts[0]
            key = parts[1]
            type_name = parts[2] if len(parts) > 2 else "single_line_text_field"
            
            create_metafield(product_id, namespace, key, value, type_name)

def check_product_exists(handle):
    """
    Checks if a product exists using its handle.
    Returns the product's GID, a dict of its variant SKUs, existing option names, and default variant ID.
    """
    query = """
    query productByHandle($handle: String!) {
      productByHandle(handle: $handle) {
        id
        options {
          name
        }
        variants(first: 250) {
          edges {
            node {
              id
              sku
              title
              inventoryItem {
                id
              }
            }
          }
        }
      }
    }
    """
    variables = {'handle': handle}
    data = run_graphql_query(query, variables)
    
    if data and data.get('productByHandle'):
        product_data = data['productByHandle']
        product_id = product_data['id']
        variants = {}
        default_variant_id = None
        
        existing_options = []
        if product_data.get('options'):
            existing_options = [opt['name'] for opt in product_data['options']]
        
        variants_list = product_data.get('variants', {}).get('edges', [])
        has_options = len(existing_options) > 0
        
        for edge in variants_list:
            variant_node = edge['node']
            sku = variant_node['sku']
            title = variant_node.get('title', '')
            
            if not has_options and len(variants_list) == 1:
                default_variant_id = variant_node['id']
            elif title == 'Default Title' and not has_options:
                default_variant_id = variant_node['id']
            
            if sku:
                variants[str(sku).strip()] = variant_node['id']
        
        return product_id, variants, existing_options, default_variant_id
        
    return None, {}, [], None

def create_product_with_variants(group_df):
    """
    Creates a new product, then creates options, then adds all variants.
    ProductInput doesn't accept options or variants directly.
    """
    main_row = group_df.iloc[0]
    print(f"Creating new product: {main_row['title']} with {len(group_df)} variant(s)...")
    
    mutation = """
    mutation productCreate($input: ProductInput!) {
      productCreate(input: $input) {
        product {
          id
          title
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    product_input = {
        "handle": main_row['handle'],
        "title": main_row['title'],
        "descriptionHtml": str(main_row.get('description', '')),
        "vendor": str(main_row.get('vendor', '')),
        "productType": str(main_row.get('productType', '')),
        "tags": [tag.strip() for tag in str(main_row.get('tags', '')).split(',') if tag.strip()]
    }
        
    variables = {
        "input": product_input
    }

    data = run_graphql_query(mutation, variables)
    if not data or not data.get('productCreate') or not data['productCreate']['product']:
        return None
    
    product_id = data['productCreate']['product']['id']
    print(f"Successfully created product ID: {product_id}")
    
    option_name = None
    if not pd.isna(main_row.get('variant_option1_name', '')):
        option_name = str(main_row['variant_option1_name']).strip()
    
    option_values = [str(row['variant_option1_value']) for _, row in group_df.iterrows() 
                    if not pd.isna(row.get('variant_option1_value', ''))]
    unique_values = list(dict.fromkeys(option_values))  # Preserve order, remove duplicates
    
    if option_name and unique_values:
        options_mutation = """
        mutation productOptionsCreate($productId: ID!, $options: [OptionCreateInput!]!) {
          productOptionsCreate(productId: $productId, options: $options) {
            product {
              id
              options {
                id
                name
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        
        values_objects = [{"name": val} for val in unique_values]
        options_input = [{
            "name": option_name,
            "values": values_objects
        }]
        
        options_variables = {
            "productId": product_id,
            "options": options_input
        }
        
        options_data = run_graphql_query(options_mutation, options_variables)
        if options_data and options_data.get('productOptionsCreate'):
            if options_data['productOptionsCreate'].get('userErrors'):
                print(f"Warning creating options: {options_data['productOptionsCreate']['userErrors']}")
            else:
                print(f"Successfully created option: {option_name} (variants auto-created)")
                time.sleep(2)
    
    query_variants = """
    query getProductVariants($id: ID!) {
      product(id: $id) {
        id
        variants(first: 250) {
          edges {
            node {
              id
              sku
              price
              inventoryItem {
                id
              }
              selectedOptions {
                name
                value
              }
            }
          }
        }
      }
    }
    """
    
    existing_variants_data = None
    for attempt in range(3):
        existing_variants_data = run_graphql_query(query_variants, {'id': product_id})
        if existing_variants_data and existing_variants_data.get('product'):
            has_inventory_items = False
            for edge in existing_variants_data['product']['variants']['edges']:
                variant = edge['node']
                if variant.get('inventoryItem') and variant['inventoryItem'].get('id'):
                    has_inventory_items = True
                    break
            if has_inventory_items:
                break
        if attempt < 2:
            print("Waiting for inventory items to be ready...")
            time.sleep(2)
    
    variants_map = {}
    variant_inventory_map = {}  
    
    if existing_variants_data and existing_variants_data.get('product'):
        found_option_values = []
        for edge in existing_variants_data['product']['variants']['edges']:
            variant = edge['node']
            variant_id = variant['id']
            if variant.get('selectedOptions') and len(variant['selectedOptions']) > 0:
                option_value = str(variant['selectedOptions'][0]['value']).strip()
                found_option_values.append(option_value)
                variants_map[option_value] = variant_id
                variants_map[option_value.lower()] = variant_id
            if variant.get('inventoryItem') and variant['inventoryItem'].get('id'):
                variant_inventory_map[variant_id] = variant['inventoryItem']['id']
            else:
                print(f"Warning: No inventory item ID for variant {variant_id}, will query later")
        
        if found_option_values:
            print(f"Found variants with option values: {', '.join(found_option_values)}")
    
    variants_to_update = []
    variant_sku_map = {}
    
    for _, row in group_df.iterrows():
        sku = row.get('variant_sku', '')
        if not sku or pd.isna(sku):
            print("Skipping variant, no SKU provided.")
            continue
        
        option_value = None
        if option_name and not pd.isna(row.get('variant_option1_value', '')):
            option_value = str(row['variant_option1_value']).strip()
        
        variant_id = None
        if option_value:
            variant_id = variants_map.get(option_value) or variants_map.get(option_value.lower())
           
        if variant_id:
            variant_data = {
                "id": variant_id,
                "price": str(row.get('variant_price', '0.00'))
            }
            variants_to_update.append(variant_data)
            inventory_item_id = variant_inventory_map.get(variant_id)
            if inventory_item_id:
                variant_sku_map[variant_id] = (inventory_item_id, str(sku))
            else:
                variant_sku_map[variant_id] = (None, str(sku))
        else:
            if not option_name:
                if existing_variants_data and existing_variants_data.get('product'):
                    default_variant = None
                    for edge in existing_variants_data['product']['variants']['edges']:
                        variant = edge['node']
                        if not variant.get('selectedOptions') or len(variant['selectedOptions']) == 0:
                            default_variant = variant
                            break
                    
                    if default_variant:
                        variant_id = default_variant['id']
                        variant_data = {
                            "id": variant_id,
                            "price": str(row.get('variant_price', '0.00'))
                        }
                        variants_to_update.append(variant_data)
                        inventory_item_id = variant_inventory_map.get(variant_id)
                        if inventory_item_id:
                            variant_sku_map[variant_id] = (inventory_item_id, str(sku))
                        else:
                            variant_sku_map[variant_id] = (None, str(sku))
           
    if variants_to_update:
        update_mutation = """
        mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants {
              id
              price
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        
        update_data = run_graphql_query(update_mutation, {
            'productId': product_id,
            'variants': variants_to_update
        })
        if update_data and update_data.get('productVariantsBulkUpdate'):
            if update_data['productVariantsBulkUpdate'].get('productVariants'):
                updated_variants = update_data['productVariantsBulkUpdate']['productVariants']
                print(f"Successfully updated {len(updated_variants)} variant(s) with price")
                for variant in updated_variants:
                    variant_id = variant['id']
                    if variant_id in variant_sku_map:
                        inventory_item_id, sku = variant_sku_map[variant_id]
                        if inventory_item_id:
                            if update_variant_sku_individual(inventory_item_id, sku):
                                print(f"Successfully updated SKU for variant {variant_id}")
                        else:
                            if update_variant_sku(variant_id, sku):
                                print(f"Successfully updated SKU for variant {variant_id}")
                            else:
                                print(f"Failed to update SKU for variant {variant_id}")

        else:
          pass
    
    if 'images' in main_row:
        add_images_to_product(product_id, main_row['images'])
    
    add_metafields_to_product(product_id, main_row)
    
    publish_product_to_sales_channel(product_id)
    
    return product_id

def update_product(product_id, row_data):
    """
    Updates an existing product's core details.
    """
    print(f"Updating product: {row_data['title']} (ID: {product_id})...")
    mutation = """
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    input_vars = {
        "id": product_id,
        "handle": row_data['handle'],
        "title": row_data['title'],
        "descriptionHtml": str(row_data.get('description', '')),
        "vendor": str(row_data.get('vendor', '')),
        "productType": str(row_data.get('productType', '')),
        "tags": [tag.strip() for tag in str(row_data.get('tags', '')).split(',') if tag.strip()]
    }
    
    data = run_graphql_query(mutation, {'input': input_vars})
    if data and data.get('productUpdate') and data['productUpdate']['product']:
        print("Successfully updated product.")
        
        if 'images' in row_data:
            add_images_to_product(product_id, row_data['images'])
        
        add_metafields_to_product(product_id, row_data)
    else:
      pass

def publish_product_to_sales_channel(product_id):
    """
    Publishes product to Online Store sales channel.
    """
    query = """
    query getPublications {
      publications(first: 10) {
        edges {
          node {
            id
            name
          }
        }
      }
    }
    """
    
    data = run_graphql_query(query, {})
    if not data or not data.get('publications'):
        return
    
    online_store_publication_id = None
    for edge in data['publications']['edges']:
        if edge['node']['name'] == 'Online Store':
            online_store_publication_id = edge['node']['id']
            break
    
    if not online_store_publication_id:
        return
    
    mutation = """
    mutation publishablePublish($id: ID!, $input: [PublicationInput!]!) {
      publishablePublish(id: $id, input: $input) {
        publishable {
          ... on Product {
            id
            title
          }
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    variables = {
        "id": product_id,
        "input": [{
            "publicationId": online_store_publication_id
        }]
    }
    
    publish_data = run_graphql_query(mutation, variables)
    if publish_data and publish_data.get('publishablePublish'):
        if publish_data['publishablePublish'].get('userErrors'):
          pass
        else:
            print("Successfully published product to Online Store")
    else:
      pass

def ensure_product_options_exist(product_id, option_name, option_values):
    """
    Ensures product options exist. Creates them if they don't.
    """
    if not option_name or not option_values:
        return True
    
    query = """
    query getProduct($id: ID!) {
      product(id: $id) {
        id
        options {
          name
        }
      }
    }
    """
    data = run_graphql_query(query, {'id': product_id})
    
    existing_option_names = []
    if data and data.get('product') and data['product'].get('options'):
        existing_option_names = [opt['name'] for opt in data['product']['options']]
    
    if option_name not in existing_option_names:
        options_mutation = """
        mutation productOptionsCreate($productId: ID!, $options: [OptionCreateInput!]!) {
          productOptionsCreate(productId: $productId, options: $options) {
            product {
              id
              options {
                id
                name
              }
            }
            userErrors {
              field
              message
            }
          }
        }
        """
        
        unique_values = list(dict.fromkeys(option_values))
        values_objects = [{"name": val} for val in unique_values]
        options_input = [{
            "name": option_name,
            "values": values_objects
        }]
        
        options_variables = {
            "productId": product_id,
            "options": options_input
        }
        
        options_data = run_graphql_query(options_mutation, options_variables)
        if options_data and options_data.get('productOptionsCreate'):
            if options_data['productOptionsCreate'].get('userErrors'):
                return False
            else:
                print(f"Successfully created option: {option_name}")
                return True
        return False
    
    return True

def update_variant_sku_individual(inventory_item_id, sku):
    """
    Updates a variant's SKU using inventoryItemUpdate mutation.
    """
    if not inventory_item_id:
        return False

    mutation = """
    mutation inventoryItemUpdate($input: InventoryItemInput!) {
      inventoryItemUpdate(input: $input) {
        inventoryItem {
          id
          sku
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    inventory_input = {
        "id": inventory_item_id,
        "sku": sku
    }
    
    data = run_graphql_query(mutation, {'input': inventory_input})
    if data and data.get('inventoryItemUpdate'):
        if data['inventoryItemUpdate'].get('inventoryItem'):
            return True
        if data['inventoryItemUpdate'].get('userErrors'):
           pass 
    return False

def update_variant_sku(variant_id, sku):
    """
    Updates a variant's SKU by first getting its inventory item ID.
    """
    query = """
    query getVariant($id: ID!) {
      productVariant(id: $id) {
        id
        inventoryItem {
          id
        }
      }
    }
    """
    
    data = run_graphql_query(query, {'id': variant_id})
    
    if data and data.get('productVariant') and data['productVariant'].get('inventoryItem'):
        inventory_item_id = data['productVariant']['inventoryItem']['id']
        return update_variant_sku_individual(inventory_item_id, sku)
    else:
        return False

def create_variant(product_id, row_data, option_name, default_variant_id=None):
    """
    Adds a new variant to an existing product.
    """
    if not option_name and default_variant_id:
        print(f"Updating default variant (ID: {default_variant_id}) instead of creating new one...")
        update_variant(default_variant_id, row_data)
        return
    
    print(f"Creating variant: {row_data.get('variant_sku', 'N/A')} for product {product_id}...")
    mutation = """
    mutation productVariantsBulkCreate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
      productVariantsBulkCreate(productId: $productId, variants: $variants) {
        productVariants {
          id
          sku
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    variant_input = {
        "price": str(row_data.get('variant_price', '0.00'))
    }
    
    if option_name and not pd.isna(row_data.get('variant_option1_value', '')):
        variant_input["optionValues"] = [
            {
                "name": str(row_data['variant_option1_value']),
                "optionName": option_name
            }
        ]
    
    variables = {
        "productId": product_id,
        "variants": [variant_input]
    }
    
    data = run_graphql_query(mutation, variables)
    if data and data.get('productVariantsBulkCreate'):
        created_variants = data['productVariantsBulkCreate'].get('productVariants', [])
        if created_variants:
            variant_id = created_variants[0]['id']
            sku = row_data.get('variant_sku', '')
            if sku and not pd.isna(sku):
                time.sleep(2)  
                update_variant_sku(variant_id, str(sku))
            print(f"Successfully created variant: {variant_id}")
        if data['productVariantsBulkCreate'].get('userErrors'):
          pass  
    else:
      pass

def update_variant(variant_id, row_data):
    """
    Updates an existing variant's price and SKU.
    """
    print(f"Updating variant: {row_data.get('variant_sku', 'N/A')} (ID: {variant_id})...")
    mutation = """
    mutation productVariantsBulkUpdate($variants: [ProductVariantsBulkInput!]!) {
      productVariantsBulkUpdate(variants: $variants) {
        productVariants {
          id
          price
        }
        userErrors {
          field
          message
        }
      }
    }
    """
    
    variant_input = {
        "id": variant_id,
        "price": str(row_data.get('variant_price', '0.00'))
    }
    
    data = run_graphql_query(mutation, {'variants': [variant_input]})
    if data and data.get('productVariantsBulkUpdate'):
        if data['productVariantsBulkUpdate'].get('productVariants'):
            print("Successfully updated variant price.")
            sku = row_data.get('variant_sku', '')
            if sku and not pd.isna(sku):
                if update_variant_sku(variant_id, str(sku)):
                    print("Successfully updated variant SKU.")
       
    else:
      pass

def main():
    """
    Main function to read Excel and process products.
    """
    try:
        df = pd.read_excel(EXCEL_FILE)
        
        required_columns = ['handle', 'title', 'variant_sku', 'variant_price']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Error: Missing required columns: {', '.join(missing_columns)}", file=sys.stderr)
            return
        
        grouped = df.groupby('handle')
        
        for handle, group_df in grouped:
            print(f"\n--- Processing Product: {handle} ---")
            
            main_row = group_df.iloc[0]
            product_id, existing_variants, existing_options, default_variant_id = check_product_exists(handle)
            
            if product_id is None:
                product_id = create_product_with_variants(group_df)
                
                if product_id is None:
                    print(f"Failed to create product {main_row['title']}. Skipping.")
                    continue
            else:
                update_product(product_id, main_row)
                
                option_name = None
                if not pd.isna(main_row.get('variant_option1_name', '')):
                    option_name = str(main_row['variant_option1_name']).strip()
                
                if option_name:
                    option_values = [str(row['variant_option1_value']) for _, row in group_df.iterrows() 
                                   if not pd.isna(row.get('variant_option1_value', ''))]
                    if option_values:
                        ensure_product_options_exist(product_id, option_name, option_values)
                
                for _, row in group_df.iterrows():
                    sku = row.get('variant_sku', '')
                    if not sku or pd.isna(sku):
                        print("Skipping variant, no SKU provided.")
                        continue
                    
                    sku = str(sku).strip()
                    variant_id = existing_variants.get(sku)
                    
                    if variant_id is None:
                        create_variant(product_id, row, option_name, default_variant_id)
                    else:
                        update_variant(variant_id, row)
            
            print(f"--- Finished Product: {handle} ---")

    except FileNotFoundError:
        print(f"Error: The file '{EXCEL_FILE}' was not found.", file=sys.stderr)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    if not SHOP_URL or not ADMIN_API_TOKEN or SHOP_URL == "your-store.myshopify.com" or ADMIN_API_TOKEN == "shpat_xxxxxxxxxxxxx":
        print("Error: SHOP_URL and ADMIN_API_TOKEN must be set at the top of the script.", file=sys.stderr)
        print("Please update the configuration section with your Shopify store details.", file=sys.stderr)
    else:
        main()
