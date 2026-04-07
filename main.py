
import os
import KalshiExtractionEngine
import json

API_KEY = os.getenv("KALSHI_API_KEY")
API_PATH = os.getenv("KALSHI_API_KEY_PATH")

engine = KalshiExtractionEngine(api_key_id=API_KEY, private_key_path=API_PATH, use_demo=False)

print("--- Extract and Map All Platform Categories ---")
taxonomy = engine.fetch_categories_taxonomy()
for cat_name, tags_array in list(taxonomy.items()):
    print(f"Parent Category: {cat_name} | Monitored Tags: {', '.join(tags_array if tags_array is not None else [])}")
print(f"Total {len(list(taxonomy.items()))} categories")

# Save taxonomy
with open("taxonomy.json", "w") as f:
    json.dump(taxonomy, f, indent=4)

print("Starting historical data extraction...")
engine.extract_with_checkpointing()
print("Extraction complete!")
