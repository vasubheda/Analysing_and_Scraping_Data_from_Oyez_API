import aiohttp
import asyncio
import os
import json
from tqdm import tqdm

BASE_URL = "https://api.oyez.org/cases?per_page=0&filter=term:"
RESOLVED_FOLDER = "data/Resolved"
UNRESOLVED_FOLDER = "data/UnResolved"

# Ensure the output folders exist
os.makedirs(RESOLVED_FOLDER, exist_ok=True)
os.makedirs(UNRESOLVED_FOLDER, exist_ok=True)

async def fetch_url(session, url):
    """
    Fetch a single URL asynchronously and parse JSON data.
    """
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.json()
            else:
                print(f"Failed to fetch {url}: {response.status}")
                return None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

async def categorize_and_save(case):
    """
    Categorize the case as "Resolved" or "Unresolved" based on timeline event and save it in a folder specific to each case.
    """
    # Check if the timeline contains an event with "Decided"
    resolved = any(
        entry and entry.get("event") == "Decided" for entry in case.get("timeline", [])
    )
    
    case_id = case.get("id", "unknown_case")
    case_name = case.get("name", "unknown_case").replace(" ", "_").replace("/", "_")
    
    # Create a folder for each case using its ID and name
    output_folder = os.path.join(RESOLVED_FOLDER if resolved else UNRESOLVED_FOLDER, f"{case_id}_{case_name}")
    os.makedirs(output_folder, exist_ok=True)
    
    output_path = os.path.join(output_folder, f"{case_id}_{case_name}.json")
    
    # Save the case data in the created folder
    with open(output_path, "w") as f:
        json.dump(case, f, indent=4)

    return resolved

async def fetch_and_process_cases(start_year, end_year):
    """
    Fetch cases from Oyez API for the specified range of years and categorize them in real-time.
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        for year in range(start_year, end_year + 1):
            url = f"{BASE_URL}{year}"
            year_data = await fetch_url(session, url)
            if year_data:
                for case in year_data:
                    tasks.append(categorize_and_save(case))
        
        # Process tasks in batches to limit memory usage
        resolved_count = 0
        unresolved_count = 0
        for batch in tqdm(
            asyncio.as_completed(tasks), total=len(tasks), desc="Processing cases"
        ):
            resolved = await batch
            if resolved:
                resolved_count += 1
            else:
                unresolved_count += 1

        print(f"\nCategorization complete: {resolved_count} Resolved, {unresolved_count} Unresolved.")

async def main(start_year, end_year):
    """
    Main function to orchestrate fetching, categorizing, and saving cases.
    """
    print(f"Fetching and processing cases from {start_year} to {end_year}...")
    await fetch_and_process_cases(start_year, end_year)
    print(f"Data saved to '{RESOLVED_FOLDER}' and '{UNRESOLVED_FOLDER}'.")

if __name__ == "__main__":
    # Define the year range
    START_YEAR = 1990
    END_YEAR = 2025
    
    # Run the asyncio event loop
    asyncio.run(main(START_YEAR, END_YEAR))
