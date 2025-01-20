# import aiohttp
# import asyncio
# import os
# import json

# RESOLVED_FOLDER = "data/Resolved"

# async def fetch_case_details(session, url):
#     """
#     Fetch and parse the detailed case information from the provided href URL.
#     """
#     try:
#         async with session.get(url) as response:
#             if response.status == 200:
#                 case_data = await response.json()
                
#                 # Extract the case facts and conclusion from the detailed case data
#                 facts = case_data.get("facts_of_the_case", "values not available")
#                 conclusion = case_data.get("conclusion", "values not available")
                
#                 return facts, conclusion
#             else:
#                 print(f"Failed to fetch {url}: {response.status}")
#                 return "values not available", "values not available"
#     except Exception as e:
#         print(f"Error fetching {url}: {e}")
#         return "values not available", "values not available"

# async def update_case_json(case_folder, case_file, session):
#     """
#     Update the case JSON file inside its respective folder with the extracted facts and conclusion.
#     """
#     case_path = os.path.join(case_folder, case_file)
    
#     with open(case_path, "r") as f:
#         case = json.load(f)
    
#     # Check if the case has an href key with a URL
#     href = case.get("href")
#     if href:
#         print(f"Fetching details for case {case.get('id')} from {href}...")
#         facts, conclusion = await fetch_case_details(session, href)
        
#         # Add the facts and conclusion to the root JSON if found
#         case["facts_of_the_case"] = facts
#         case["conclusion"] = conclusion
        
#         # Save the updated case back to its respective folder
#         with open(case_path, "w") as f:
#             json.dump(case, f, indent=4)
#         print(f"Updated case {case.get('id')} with facts and conclusion.")
#     else:
#         print(f"No href found for case {case.get('id')}. Skipping...")

# async def process_resolved_cases():
#     """
#     Process each case in the resolved folder to update with facts and conclusions.
#     """
#     async with aiohttp.ClientSession() as session:
#         tasks = []
#         # Walk through each subfolder inside RESOLVED_FOLDER
#         for case_folder in os.listdir(RESOLVED_FOLDER):
#             case_folder_path = os.path.join(RESOLVED_FOLDER, case_folder)
#             if os.path.isdir(case_folder_path):  # Ensure it's a directory (case folder)
#                 for case_file in os.listdir(case_folder_path):
#                     if case_file.endswith(".json"):  # Process only JSON files
#                         tasks.append(update_case_json(case_folder_path, case_file, session))

#         await asyncio.gather(*tasks)

# async def main():
#     """
#     Main function to orchestrate updating resolved cases.
#     """
#     print("Updating resolved cases with facts and conclusions...")
#     await process_resolved_cases()
#     print("All resolved cases updated.")

# if __name__ == "__main__":
#     # Run the asyncio event loop to update all cases
#     asyncio.run(main())

import aiohttp
import asyncio
import os
import json
import time
from asyncio import Semaphore

RESOLVED_FOLDER = "data/Resolved"

# Limit concurrency to 10 concurrent requests at a time
CONCURRENCY_LIMIT = 10
semaphore = Semaphore(CONCURRENCY_LIMIT)

# Max retries for failed requests
MAX_RETRIES = 3
RETRY_DELAY = 2  # Seconds to wait before retrying

async def fetch_case_details(session, url):
    """
    Fetch and parse the detailed case information from the provided href URL with retries and a timeout.
    """
    retries = 0
    while retries < MAX_RETRIES:
        try:
            async with semaphore:
                async with session.get(url, timeout=10) as response:  # Timeout set to 10 seconds
                    if response.status == 200:
                        case_data = await response.json()
                        facts = case_data.get("facts_of_the_case", "values not available")
                        conclusion = case_data.get("conclusion", "values not available")
                        return facts, conclusion
                    else:
                        print(f"Failed to fetch {url}: {response.status}")
                        return "values not available", "values not available"
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            retries += 1
            print(f"Error fetching {url}, retrying ({retries}/{MAX_RETRIES}): {e}")
            await asyncio.sleep(RETRY_DELAY)
    return "values not available", "values not available"

async def update_case_json(case_folder, case_file, session):
    """
    Update the case JSON file inside its respective folder with the extracted facts and conclusion.
    """
    case_path = os.path.join(case_folder, case_file)
    
    with open(case_path, "r") as f:
        case = json.load(f)
    
    # Check if the case has an href key with a URL
    href = case.get("href")
    if href:
        print(f"Fetching details for case {case.get('id')} from {href}...")
        facts, conclusion = await fetch_case_details(session, href)
        
        # Add the facts and conclusion to the root JSON if found
        case["facts_of_the_case"] = facts
        case["conclusion"] = conclusion
        
        # Save the updated case back to its respective folder
        with open(case_path, "w") as f:
            json.dump(case, f, indent=4)
        print(f"Updated case {case.get('id')} with facts and conclusion.")
    else:
        print(f"No href found for case {case.get('id')}. Skipping...")

async def process_resolved_cases():
    """
    Process each case in the resolved folder to update with facts and conclusions.
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        # Walk through each subfolder inside RESOLVED_FOLDER
        for case_folder in os.listdir(RESOLVED_FOLDER):
            case_folder_path = os.path.join(RESOLVED_FOLDER, case_folder)
            if os.path.isdir(case_folder_path):  # Ensure it's a directory (case folder)
                for case_file in os.listdir(case_folder_path):
                    if case_file.endswith(".json"):  # Process only JSON files
                        tasks.append(update_case_json(case_folder_path, case_file, session))

        await asyncio.gather(*tasks)

async def main():
    """
    Main function to orchestrate updating resolved cases.
    """
    print("Updating resolved cases with facts and conclusions...")
    start_time = time.time()
    await process_resolved_cases()
    print(f"All resolved cases updated. Time taken: {time.time() - start_time} seconds.")

if __name__ == "__main__":
    # Run the asyncio event loop to update all cases
    asyncio.run(main())
