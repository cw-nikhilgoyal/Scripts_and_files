import json
import os
import warnings
from typing import List, Dict, Any
from dotenv import load_dotenv
from opensearchpy import OpenSearch
from langchain_openai import OpenAIEmbeddings

# Suppress SSL warnings
warnings.filterwarnings('ignore')

# Load environment variables
load_dotenv()

# Configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENSEARCH_HOST = os.getenv("OPENSEARCH_HOST", "localhost")
OPENSEARCH_USERNAME = os.getenv("OPENSEARCH_USERNAME", "admin")
OPENSEARCH_PASSWORD = os.getenv("OPENSEARCH_PASSWORD", "admin")
INDEX_NAME = "specs_structure"
EMBEDDING_DIMENSION = 3072


def load_specs_data(file_path: str = "specs_structure.json") -> Dict[str, Any]:
    """Load the specs structure JSON file."""
    with open(file_path, 'r') as f:
        return json.load(f)


def transform_to_documents(specs_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Transform specs data into documents suitable for embedding."""
    documents = []
    
    for field_name, field_data in specs_data.items():
        # Extract values as list of strings
        values = [v.get("key", "") for v in field_data.get("values", [])]
        tags = field_data.get("tags", [])
        synonyms = field_data.get("synonyms", [])
        
        # Create combined text for embedding
        text_parts = [field_name]
        
        if synonyms:
            text_parts.append(f"Synonyms: {', '.join(synonyms)}")
        
        if tags:
            text_parts.append(f"Tags: {', '.join(tags)}")
        
        if values:
            text_parts.append(f"Values: {', '.join(values)}")
        
        combined_text = ". ".join(text_parts)
        
        documents.append({
            "field_name": field_name,
            "text": combined_text,
            "synonyms": synonyms,
            "tags": tags,
            "values": values
        })
    
    return documents


def create_opensearch_client() -> OpenSearch:
    """Create and return an OpenSearch client."""
    auth = None
    if OPENSEARCH_USERNAME and OPENSEARCH_PASSWORD:
        auth = (OPENSEARCH_USERNAME, OPENSEARCH_PASSWORD)
    
    config = {
        'hosts': [OPENSEARCH_HOST],
        'http_auth': auth,
        'use_ssl': True,
        'verify_certs': False,
        'ssl_assert_hostname': False,
        'ssl_show_warn': False,
        'timeout': 3000
    }
    
    client = OpenSearch(**config)
    return client


def create_index_with_mapping(client: OpenSearch, index_name: str):
    """Create OpenSearch index with k-NN vector mapping."""
    index_body = {
        "settings": {
            "index": {
                "knn": True,
                "knn.algo_param.ef_search": 100
            }
        },
        "mappings": {
            "properties": {
                "field_name": {
                    "type": "keyword"
                },
                "text": {
                    "type": "text"
                },
                "embedding": {
                    "type": "knn_vector",
                    "dimension": EMBEDDING_DIMENSION,
                    "method": {
                        "name": "hnsw",
                        "space_type": "l2",
                        "engine": "nmslib",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 24
                        }
                    }
                },
                "synonyms": {
                    "type": "keyword"
                },
                "tags": {
                    "type": "keyword"
                },
                "values": {
                    "type": "keyword"
                }
            }
        }
    }
    
    # Delete index if it exists
    if client.indices.exists(index=index_name):
        print(f"Index '{index_name}' already exists. Deleting...")
        client.indices.delete(index=index_name)
    
    # Create new index
    print(f"Creating index '{index_name}'...")
    client.indices.create(index=index_name, body=index_body)
    print(f"Index '{index_name}' created successfully.")


def generate_and_index_embeddings(
    client: OpenSearch,
    documents: List[Dict[str, Any]],
    index_name: str
):
    """Generate embeddings and index documents in OpenSearch."""
    # Initialize OpenAI embeddings with LangChain
    embeddings_model = OpenAIEmbeddings(
        model="text-embedding-3-large",
        openai_api_key=OPENAI_API_KEY,
        dimensions=EMBEDDING_DIMENSION
    )
    
    print(f"Processing {len(documents)} documents...")
    
    successful_count = 0
    failed_count = 0
    
    for idx, doc in enumerate(documents, 1):
        try:
            # Generate embedding for the text
            text = doc["text"]
            embedding = embeddings_model.embed_query(text)
            
            # Add embedding to document
            doc_with_embedding = {
                **doc,
                "embedding": embedding
            }
            
            # Index document
            response = client.index(
                index=index_name,
                body=doc_with_embedding,
                id=doc["field_name"],
                refresh=True
            )
            
            successful_count += 1
            print(f"[{idx}/{len(documents)}] Indexed: {doc['field_name']}")
            
        except Exception as e:
            failed_count += 1
            print(f"[{idx}/{len(documents)}] Error indexing {doc['field_name']}: {str(e)}")
    
    print(f"\n{'='*50}")
    print(f"Indexing complete!")
    print(f"Successfully indexed: {successful_count} documents")
    print(f"Failed: {failed_count} documents")
    print(f"{'='*50}")


def main():
    """Main execution function."""
    print("Starting OpenSearch embeddings creation process...\n")
    
    try:
        # Step 1: Load and parse data
        print("Step 1: Loading specs_structure.json...")
        specs_data = load_specs_data()
        print(f"Loaded {len(specs_data)} specification fields.\n")
        
        # Step 2: Transform data to documents
        print("Step 2: Transforming data to documents...")
        documents = transform_to_documents(specs_data)
        print(f"Created {len(documents)} documents.\n")
        
        # Step 3: Create OpenSearch client
        print("Step 3: Connecting to OpenSearch...")
        client = create_opensearch_client()
        print("Connected successfully.\n")
        
        # Step 4: Create index with k-NN mapping
        print("Step 4: Creating index with k-NN mapping...")
        create_index_with_mapping(client, INDEX_NAME)
        print()
        
        # Step 5: Generate embeddings and index documents
        print("Step 5: Generating embeddings and indexing documents...")
        generate_and_index_embeddings(client, documents, INDEX_NAME)
        
        print("\n✓ Process completed successfully!")
        
    except FileNotFoundError:
        print("Error: specs_structure.json file not found!")
    except Exception as e:
        print(f"Error: {str(e)}")
        raise


if __name__ == "__main__":
    main()
