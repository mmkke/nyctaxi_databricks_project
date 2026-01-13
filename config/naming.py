

def fqtn(layer: str, table: str, use_unity_catalog: bool, catalog: str = "nyctaxi") -> str:
    if use_unity_catalog:
        return f"{catalog}.{layer}.{table}"          # e.g., nyctaxi.bronze.yellow_trips
    return f"{catalog}_{layer}.{table}"              # e.g., nyctaxi_bronze.yellow_trips