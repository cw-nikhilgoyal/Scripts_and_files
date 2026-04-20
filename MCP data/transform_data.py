#!/usr/bin/env python3
"""
Transform vehicle data into OpenSearch index formats with display fields preservation

This script creates three indexes with both normalized (lowercase) and display (original case) fields:
1. vehicle_versions - Variant-level structured metadata
2. vehicle_prices - City-wise pricing
3. model_reviews - Model-level expert summaries

Key differences from transform_to_opensearch_indexes.py:
- Adds display_{field_name} for all lowercased fields to preserve original case
- Adds display_features alongside features to preserve original feature names and values
"""

import json
import re
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path


def normalize_string(value: str) -> str:
    """Normalize string for use in IDs"""
    return re.sub(r'[^a-z0-9]+', '_', value.lower()).strip('_')


def generate_vehicle_id(make: str, model: str, version_name: str, model_year: Optional[int] = None) -> str:
    """Generate vehicle_id in format: make_model_version_year"""
    make_norm = normalize_string(make)
    model_norm = normalize_string(model)
    version_norm = normalize_string(version_name)
    
    if model_year:
        return f"{make_norm}_{model_norm}_{version_norm}_{model_year}"
    return f"{make_norm}_{model_norm}_{version_norm}"


def normalize_feature_value(value: Any) -> str:
    """Normalize feature values to strings to avoid type conflicts in OpenSearch
    All feature values are converted to lowercase strings for consistency.
    """
    if value is None:
        return None
    
    if isinstance(value, bool):
        return "true" if value else "false"
    
    if isinstance(value, (int, float)):
        return str(value).lower()
    
    if isinstance(value, str):
        value_lower = value.lower().strip()
        
        if value_lower in ['1', '0']:
            return "not available"
        
        if value_lower in ['yes', 'true', 'y']:
            return "true"
        elif value_lower in ['no', 'false', 'n', '']:
            return "false"
        return value_lower
    
    return str(value).lower()


def get_display_feature_value(value: Any) -> str:
    """Get display version of feature value (preserves original case and format)"""
    if value is None:
        return None
    
    if isinstance(value, bool):
        return "Yes" if value else "No"
    
    if isinstance(value, (int, float)):
        return str(value)
    
    if isinstance(value, str):
        value_stripped = value.strip()
        
        # Preserve original string as-is
        if value_stripped:
            return value_stripped
        return None
    
    return str(value)


def parse_value(value: str, data_type: str) -> Any:
    """Parse feature value based on data type"""
    if data_type == "Boolean":
        return value.strip() == "1"
    elif data_type in ["Integer", "Number"]:
        try:
            return int(value) if '.' not in value else float(value)
        except (ValueError, TypeError):
            return value
    else:
        return value


def flatten_feature(spec_name: str, value: Any) -> Tuple[str, Any, str]:
    """Flatten feature and return (normalized_key, value, original_key)"""
    # Store original spec name for display
    original_key = spec_name.strip()
    
    # Clean spec name: remove content in parentheses for normalized key
    cleaned_name = spec_name
    cleaned_name = re.sub(r'\s*\([^)]*\)', '', cleaned_name)
    cleaned_name = cleaned_name.strip()
    
    # Normalize to create clean key
    spec_norm = normalize_string(cleaned_name)
    
    return spec_norm, value, original_key


def extract_features_text(features: Dict[str, Any]) -> str:
    """Generate searchable text from features"""
    text_parts = []
    
    for key, value in features.items():
        if isinstance(value, bool) and value:
            feature_name = key.replace('_', ' ')
            text_parts.append(feature_name)
        elif isinstance(value, (int, float)):
            feature_name = key.replace('_', ' ')
            text_parts.append(f"{feature_name} {value}")
        elif isinstance(value, str) and value and value.lower() not in ("no", "0", "false"):
            text_parts.append(value)
    
    return ", ".join(text_parts)


def extract_model_year(version_name: str, version_launchedon: Optional[str] = None) -> Optional[int]:
    """Extract model year from version name or launch date"""
    year_match = re.search(r'\b(20\d{2})\b', version_name)
    if year_match:
        return int(year_match.group(1))
    
    if version_launchedon:
        try:
            date_obj = datetime.strptime(version_launchedon, "%Y-%m-%d")
            return date_obj.year
        except (ValueError, TypeError):
            pass
    
    return None


def lowercase_if_string(value: Any) -> Any:
    """Safely lowercase string values, return non-strings as-is"""
    return value.lower() if isinstance(value, str) and value else value


def transform_vehicle_versions(specs_data: List[Dict], model_data: List[Dict]) -> List[Dict]:
    """
    Transform data into vehicle_versions index format with display fields
    
    Schema (includes both normalized and display fields):
    - vehicle_id (keyword)
    - make (keyword) + display_make (keyword)
    - model (keyword) + display_model (keyword)
    - version_id (keyword)
    - version_name (text + keyword) + display_version_name (text + keyword)
    - model_year (integer)
    - segment (keyword) + display_segment (keyword)
    - body_style (keyword) + display_body_style (keyword)
    - fuel_type (keyword) + display_fuel_type (keyword)
    - transmission (keyword) + display_transmission (keyword)
    - model_trim (keyword) + display_model_trim (keyword)
    - version_status (keyword) + display_version_status (keyword)
    - displacement (string) + display_displacement (string)
    - features (flattened object) + display_features (object with original keys/values)
    - features_text (text)
    - ex_showroom_price (double)
    - onroad_price_delhi (double)
    - last_updated (date)
    """
    print("Transforming vehicle_versions index with display fields...")
    
    # Build version metadata lookup from model_data
    version_metadata = {}
    for model in model_data:
        # Preserve original values
        display_make = model.get("make_name", "")
        display_model = model.get("model_name", "")
        display_segment = model.get("segment", "")
        
        # Create lowercase versions
        make = display_make.lower()
        model_name = display_model.lower()
        
        model_popularity = model.get("model_popularity")
        
        for version in model.get("version_data", []):
            version_id = str(version.get("versionid", ""))
            
            # Preserve original values for all fields
            display_version_name = version.get("version_name", "")
            display_bodystyle = version.get("bodystyle", "")
            display_fueltype = version.get("fueltype", "")
            display_transmission = version.get("transmission", "")
            display_model_trim = version.get("model_trim", "")
            display_version_status = version.get("version_status", "")
            display_displacement = version.get("displacement", "")
            
            version_metadata[version_id] = {
                "make": make,
                "display_make": display_make,
                "model": model_name,
                "display_model": display_model,
                "segment": display_segment,
                "display_segment": display_segment,
                "version_name": display_version_name,
                "display_version_name": display_version_name,
                "bodystyle": display_bodystyle,
                "display_bodystyle": display_bodystyle,
                "model_year": extract_model_year(
                    display_version_name,
                    version.get("version_launchedon")
                ),
                "exshowroom_price": version.get("exshowroom_price"),
                "avg_onroadprice": version.get("avg_onroadprice"),
                "version_launchedon": version.get("version_launchedon"),
                "fueltype": display_fueltype,
                "display_fueltype": display_fueltype,
                "transmission": display_transmission,
                "display_transmission": display_transmission,
                "model_trim": display_model_trim,
                "display_model_trim": display_model_trim,
                "version_status": display_version_status,
                "display_version_status": display_version_status,
                "version_discontinuedon": version.get("version_discontinuedon"),
                "version_popularity": version.get("version_popularity"),
                "model_popularity": model_popularity,
                "displacement": display_displacement,
                "display_displacement": display_displacement,
            }
    
    # Build features by version (track both normalized and display versions)
    version_features = defaultdict(lambda: {
        "features": {}, 
        "display_features": {},
        "categories": set()
    })
    
    for spec_entry in specs_data:
        make = spec_entry.get("Make", "").lower()
        model = spec_entry.get("Model", "").lower()
        
        # Handle two different data structures
        if "Specs" in spec_entry and isinstance(spec_entry.get("Specs"), list):
            # New format: iterate through Specs list
            for spec_group in spec_entry.get("Specs", []):
                category = spec_group.get("Category", "")
                spec_details = spec_group.get("Spec_Details", [])
                
                for spec_detail in spec_details:
                    spec_name = spec_detail.get("Specs/Features", "")
                    data_type = spec_detail.get("DataTypeName", "")
                    
                    for version_info in spec_detail.get("Versions", []):
                        version_id = str(version_info.get("VersionId", ""))
                        value = version_info.get("Value", "")
                        
                        if version_id and value and spec_name:
                            parsed_value = parse_value(value, data_type)
                            key, flattened_value, original_key = flatten_feature(spec_name, parsed_value)
                            
                            # Normalized feature value
                            normalized_value = normalize_feature_value(flattened_value)
                            if normalized_value is not None:
                                version_features[version_id]["features"][key] = normalized_value
                            
                            # Display feature value (original)
                            display_value = get_display_feature_value(flattened_value)
                            if display_value is not None:
                                version_features[version_id]["display_features"][original_key] = display_value
                            
                            version_features[version_id]["categories"].add(category)
        else:
            # Old format: Category and Spec_Details at top level
            category = spec_entry.get("Category", "")
            
            for spec_detail in spec_entry.get("Spec_Details", []):
                spec_name = spec_detail.get("Specs/Features", "")
                data_type = spec_detail.get("DataTypeName", "")
                
                for version_info in spec_detail.get("Versions", []):
                    version_id = str(version_info.get("VersionId", ""))
                    value = version_info.get("Value", "")
                    
                    if version_id and value and spec_name:
                        parsed_value = parse_value(value, data_type)
                        key, flattened_value, original_key = flatten_feature(spec_name, parsed_value)
                        
                        # Normalized feature value
                        normalized_value = normalize_feature_value(flattened_value)
                        if normalized_value is not None:
                            version_features[version_id]["features"][key] = normalized_value
                        
                        # Display feature value (original)
                        display_value = get_display_feature_value(flattened_value)
                        if display_value is not None:
                            version_features[version_id]["display_features"][original_key] = display_value
                        
                        version_features[version_id]["categories"].add(category)
    
    # Combine into final documents
    vehicle_versions = []
    processed_versions = set()
    
    for version_id, metadata in version_metadata.items():
        if version_id in processed_versions:
            continue
        
        make = metadata["make"]
        model = metadata["model"]
        version_name = metadata["version_name"]
        model_year = metadata.get("model_year")
        
        vehicle_id = generate_vehicle_id(make, model, version_name, model_year)
        
        # Get features for this version
        raw_features = version_features.get(version_id, {}).get("features", {})
        raw_display_features = version_features.get(version_id, {}).get("display_features", {})
        
        # Normalize all feature values
        features = {k: normalize_feature_value(v) for k, v in raw_features.items() if normalize_feature_value(v) is not None}
        display_features = {k: v for k, v in raw_display_features.items() if v is not None}
        features_text = extract_features_text(features)
        
        # Get prices
        ex_showroom_price = metadata.get("exshowroom_price")
        onroad_price_delhi = metadata.get("avg_onroadprice")
        
        doc = {
            "vehicle_id": vehicle_id,
            "make": make,
            "display_make": metadata.get("display_make", ""),
            "model": model,
            "display_model": metadata.get("display_model", ""),
            "version_id": version_id,
            "version_name": lowercase_if_string(version_name),
            "display_version_name": metadata.get("display_version_name", ""),
            "segment": lowercase_if_string(metadata.get("segment", "")),
            "display_segment": metadata.get("display_segment", ""),
            "body_style": lowercase_if_string(metadata.get("bodystyle", "")),
            "display_body_style": metadata.get("display_bodystyle", ""),
            "fuel_type": lowercase_if_string(metadata.get("fueltype", "")),
            "display_fuel_type": metadata.get("display_fueltype", ""),
            "transmission": lowercase_if_string(metadata.get("transmission", "")),
            "display_transmission": metadata.get("display_transmission", ""),
            "model_trim": lowercase_if_string(metadata.get("model_trim", "")),
            "display_model_trim": metadata.get("display_model_trim", ""),
            "version_status": lowercase_if_string(metadata.get("version_status", "")),
            "display_version_status": metadata.get("display_version_status", ""),
            "features": features,
            "display_features": display_features,
            "features_text": features_text,
            "last_updated": datetime.now().strftime("%Y-%m-%d")
        }
        
        # Add optional fields
        if model_year is not None:
            doc["model_year"] = model_year
        if metadata.get("version_launchedon"):
            doc["version_launchedon"] = metadata.get("version_launchedon")
        if metadata.get("version_discontinuedon"):
            doc["version_discontinuedon"] = metadata.get("version_discontinuedon")
        if metadata.get("version_popularity") is not None:
            doc["version_popularity"] = metadata.get("version_popularity")
        if metadata.get("model_popularity") is not None:
            doc["model_popularity"] = metadata.get("model_popularity")
        if metadata.get("displacement"):
            doc["displacement"] = lowercase_if_string(metadata.get("displacement"))
            doc["display_displacement"] = metadata.get("display_displacement", "")
        if ex_showroom_price is not None:
            doc["ex_showroom_price"] = ex_showroom_price
        if onroad_price_delhi is not None:
            doc["onroad_price_delhi"] = onroad_price_delhi
        
        vehicle_versions.append(doc)
        processed_versions.add(version_id)
    
    print(f"Created {len(vehicle_versions)} vehicle_versions documents with display fields")
    return vehicle_versions


def transform_vehicle_prices(model_data: List[Dict], pic_data: Optional[List[Dict]] = None, cities: Optional[List[str]] = None) -> List[Dict]:
    """
    Transform data into vehicle_prices index format with display fields
    
    Schema:
    - vehicle_id (keyword)
    - version_id (keyword)
    - version_name (text + keyword)
    - city (keyword)
    - onroad_price (double)
    - updated_at (date)
    - display_make (keyword)
    - display_model (keyword)
    """
    print("Transforming vehicle_prices index with display fields...")
    
    # Extract all unique cities from pic_data if available
    if cities is None:
        if pic_data:
            cities_set = set()
            for pic_entry in pic_data:
                city_based_prices = pic_entry.get("city_based_prices", [])
                for city_price in city_based_prices:
                    city_name = city_price.get("cityname")
                    if city_name:
                        cities_set.add(city_name)
            cities = sorted(list(cities_set))
            print(f"Extracted {len(cities)} unique cities from pic_data")
        else:
            cities = []
            print("WARNING: No pic_data provided and no cities specified. No price entries will be created.")
    
    # Build city-price lookup from pic_data
    city_prices_lookup = defaultdict(dict)
    if pic_data:
        print("Building city-price lookup from pic_data...")
        for pic_entry in pic_data:
            city_based_prices = pic_entry.get("city_based_prices", [])
            for city_price in city_based_prices:
                city_name = city_price.get("cityname")
                if not city_name:
                    continue
                
                version_prices = city_price.get("version_prices", [])
                for version_price in version_prices:
                    version_id = str(version_price.get("versionid", ""))
                    onroad_price = version_price.get("onroad_price")
                    
                    if version_id and onroad_price is not None:
                        city_prices_lookup[version_id][city_name] = onroad_price
        
        print(f"Built city-price lookup for {len(city_prices_lookup)} versions")
    
    vehicle_prices = []
    fallback_count = 0
    
    for model in model_data:
        # Preserve original values
        display_make = model.get("make_name", "")
        display_model = model.get("model_name", "")
        
        make = display_make.lower()
        model_name = display_model.lower()
        
        for version in model.get("version_data", []):
            version_id = str(version.get("versionid", ""))
            version_name = version.get("version_name", "")
            model_year = extract_model_year(
                version_name,
                version.get("version_launchedon")
            )
            
            vehicle_id = generate_vehicle_id(make, model_name, version_name, model_year)
            
            for city in cities:
                onroad_price = None
                if version_id in city_prices_lookup and city in city_prices_lookup[version_id]:
                    onroad_price = city_prices_lookup[version_id][city]
                else:
                    onroad_price = version.get("avg_onroadprice")
                    if onroad_price is not None:
                        fallback_count += 1
                
                if onroad_price is not None:
                    doc = {
                        "vehicle_id": vehicle_id,
                        "version_id": version_id,
                        "version_name": version_name,
                        "city": city,
                        "onroad_price": onroad_price,
                        "updated_at": datetime.now().strftime("%Y-%m-%d"),
                        "display_make": display_make,
                        "display_model": display_model
                    }
                    vehicle_prices.append(doc)
    
    print(f"Created {len(vehicle_prices)} vehicle_prices documents with display fields")
    if fallback_count > 0:
        print(f"WARNING: {fallback_count} entries used average price as fallback.")
    else:
        print("SUCCESS: All prices are city-specific from pic_data.")
    return vehicle_prices


def extract_expert_summary(summary: str) -> str:
    """Extract expert summary from review"""
    return summary.strip()


def transform_model_reviews(reviews_data: List[Dict]) -> List[Dict]:
    """
    Transform data into model_reviews index format with display fields
    
    Schema:
    - make (keyword) + display_make (keyword)
    - model (keyword) + display_model (keyword)
    - model_id (keyword)
    - expert_summary (text)
    - last_updated (date)
    """
    print("Transforming model_reviews index with display fields...")
    
    model_reviews = []
    
    for review in reviews_data:
        # Preserve original values
        display_make = review.get("make_name", "")
        display_model = review.get("model_name", "")
        
        make = display_make.lower()
        model = display_model.lower()
        model_id = str(int(review.get("ModelId", 0))) if review.get("ModelId") else ""
        summary = review.get("summary", "")
        
        if not make or not model or not summary:
            continue
        
        expert_summary = extract_expert_summary(summary)
        
        doc = {
            "make": make,
            "display_make": display_make,
            "model": model,
            "display_model": display_model,
            "model_id": model_id,
            "expert_summary": expert_summary,
            "last_updated": datetime.now().strftime("%Y-%m-%d")
        }
        
        model_reviews.append(doc)
    
    print(f"Created {len(model_reviews)} model_reviews documents with display fields")
    return model_reviews


def process_data_directory(base_path: Path, output_path: Path, data_source_name: str = ""):
    """Process data from a directory and save to output path"""
    print(f"\n{'=' * 60}")
    if data_source_name:
        print(f"Processing {data_source_name} data...")
    else:
        print("Processing data...")
    print("=" * 60)
    
    # Load source data
    print("\nLoading source data...")
    try:
        specs_file = base_path / "specs_data.json"
        if not specs_file.exists():
            specs_file = base_path / "specs.json"
        if not specs_file.exists():
            raise FileNotFoundError(f"Neither specs_data.json nor specs.json found in {base_path}")
        
        with open(specs_file, "r", encoding="utf-8") as f:
            specs_data = json.load(f)
        print(f"Loaded {len(specs_data)} spec entries from {specs_file.name}")
        
        with open(base_path / "model_data.json", "r", encoding="utf-8") as f:
            model_data = json.load(f)
        print(f"Loaded {len(model_data)} model entries")
        
        reviews_file = base_path / "reviews_data.json"
        if not reviews_file.exists():
            reviews_file = base_path / "reviews.json"
        if not reviews_file.exists():
            raise FileNotFoundError(f"Neither reviews_data.json nor reviews.json found in {base_path}")
        
        with open(reviews_file, "r", encoding="utf-8") as f:
            reviews_data = json.load(f)
        print(f"Loaded {len(reviews_data)} review entries from {reviews_file.name}")
        
        pic_data = None
        pic_data_file = base_path / "pic_to_150.json"
        if not pic_data_file.exists():
            pic_data_file = base_path / "pic_data.json"
        if pic_data_file.exists():
            with open(pic_data_file, "r", encoding="utf-8") as f:
                pic_data = json.load(f)
            print(f"Loaded {pic_data_file.name} with {len(pic_data)} entries")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print(f"Please ensure all data files are in the '{base_path}' directory")
        return False
    
    # Transform data
    print("\n" + "=" * 60)
    vehicle_versions = transform_vehicle_versions(specs_data, model_data)
    vehicle_prices = transform_vehicle_prices(model_data, pic_data=pic_data)
    model_reviews = transform_model_reviews(reviews_data)
    
    # Save transformed data
    print("\n" + "=" * 60)
    print("Saving transformed indexes...")
    output_path.mkdir(exist_ok=True, parents=True)
    
    with open(output_path / "vehicle_versions.json", "w", encoding="utf-8") as f:
        json.dump(vehicle_versions, f, indent=2, ensure_ascii=False)
    print(f"Saved vehicle_versions.json ({len(vehicle_versions)} documents)")
    
    with open(output_path / "vehicle_prices.json", "w", encoding="utf-8") as f:
        json.dump(vehicle_prices, f, indent=2, ensure_ascii=False)
    print(f"Saved vehicle_prices.json ({len(vehicle_prices)} documents)")
    
    with open(output_path / "model_reviews.json", "w", encoding="utf-8") as f:
        json.dump(model_reviews, f, indent=2, ensure_ascii=False)
    print(f"Saved model_reviews.json ({len(model_reviews)} documents)")
    
    # Save sample documents for verification
    print("\n" + "=" * 60)
    print("Creating sample documents for verification...")
    
    sample_output = {
        "vehicle_versions_sample": vehicle_versions[:3] if vehicle_versions else [],
        "vehicle_prices_sample": vehicle_prices[:5] if vehicle_prices else [],
        "model_reviews_sample": model_reviews[:3] if model_reviews else []
    }
    
    with open(output_path / "samples.json", "w", encoding="utf-8") as f:
        json.dump(sample_output, f, indent=2, ensure_ascii=False)
    print("Saved samples.json")
    
    print("\n" + "=" * 60)
    print("Transformation complete!")
    print(f"Output directory: {output_path}")
    print("=" * 60)
    return True


def main():
    """Main transformation function"""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Transform vehicle data into OpenSearch index formats with display fields"
    )
    parser.add_argument(
        "--source-dir",
        help="Source directory containing data files (default: processes Jan_2026_car_data)"
    )
    parser.add_argument(
        "--output-dir",
        help="Output directory for transformed JSON files"
    )
    parser.add_argument(
        "--data-source-name",
        default="",
        help="Name of the data source (for logging purposes)"
    )
    
    args = parser.parse_args()
    
    # If source and output dirs are provided, process only that
    if args.source_dir and args.output_dir:
        base_path = Path(args.source_dir)
        output_path = Path(args.output_dir)
        data_source_name = args.data_source_name or base_path.name
        process_data_directory(base_path, output_path, data_source_name)
        return
    
    # Default behavior: process Jan_2026_car_data
    print("=" * 60)
    print("OpenSearch Index Transformation Script (with Display Fields)")
    print("=" * 60)
    
    jan_2026_path = Path(__file__).parent / "Jan_2026_car_data"
    if jan_2026_path.exists():
        print("\n" + "=" * 60)
        print("Processing Jan_2026_car_data")
        print("=" * 60)
        jan_2026_output = Path(__file__).parent / "opensearch_indexes_with_display"
        process_data_directory(jan_2026_path, jan_2026_output, "Jan_2026_car_data")
    else:
        print(f"Error: Jan_2026_car_data directory not found at {jan_2026_path}")
        print("Please specify --source-dir and --output-dir arguments")


if __name__ == "__main__":
    main()
