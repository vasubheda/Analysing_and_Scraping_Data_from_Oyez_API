import aiohttp
import asyncio
import os
import json
import os
import time
from urllib.parse import urlparse

RESOLVED_FOLDER = "data/Resolved"
ATTORNEYS_FOLDER = "attorneys"  # Folder where attorney images will be stored

# Limit concurrency to 5 concurrent requests for images
CONCURRENCY_LIMIT = 5
semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

# Timeout and retries
MAX_RETRIES = 3
RETRY_DELAY = 2  # Seconds to wait before retrying

async def fetch_case_details(session, url):
    """
    Fetch the detailed case information from the provided href URL and return the advocates.
    """
    try:
        async with semaphore:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    case_data = await response.json()
                    advocates = case_data.get("advocates", [])
                    return advocates
                else:
                    print(f"Failed to fetch {url}: {response.status}")
                    return []
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return []

async def fetch_advocate_details(session, advocate_href):
    """
    Fetch advocate details from their respective href URL.
    """
    try:
        async with semaphore:
            async with session.get(advocate_href, timeout=10) as response:
                if response.status == 200:
                    advocate_data = await response.json()
                    return advocate_data
                else:
                    print(f"Failed to fetch {advocate_href}: {response.status}")
                    return None
    except Exception as e:
        print(f"Error fetching {advocate_href}: {e}")
        return None

async def fetch_and_save_image(session, image_url, attorney_folder, image_name):
    """
    Download and save the advocate's image to the attorney folder.
    """
    try:
        async with semaphore:
            async with session.get(image_url, timeout=10) as response:
                if response.status == 200:
                    # Create the attorney folder if it doesn't exist
                    os.makedirs(attorney_folder, exist_ok=True)
                    image_path = os.path.join(attorney_folder, image_name)
                    
                    # Save the image
                    with open(image_path, "wb") as f:
                        f.write(await response.read())
                    print(f"Saved image for advocate at {image_path}")
                    return image_path
                else:
                    print(f"Failed to fetch image from {image_url}")
                    return None
    except Exception as e:
        print(f"Error downloading image: {e}")
        return None

async def update_attorney_json(attorney_data, attorney_folder, image_path):
    """
    Update the attorney's JSON file with their details and image path.
    """
    attorney_data["image"] = image_path or "Image not available"
    attorney_json_path = os.path.join(attorney_folder, f"{attorney_data['name'].replace(' ', '_')}.json")
    
    with open(attorney_json_path, "w") as f:
        json.dump(attorney_data, f, indent=4)
    print(f"Updated attorney information in {attorney_json_path}")

async def process_advocates(case_folder, case_file, session):
    """
    Process the case's advocates and save their images and information.
    """
    case_path = os.path.join(case_folder, case_file)
    with open(case_path, "r") as f:
        case = json.load(f)
    
    # Get the href from the case data
    href = case.get("href")
    if href:
        print(f"Fetching advocates for case {case.get('id')}...")
        advocates = await fetch_case_details(session, href)
        
        if advocates:
            attorney_folder = os.path.join(case_folder, ATTORNEYS_FOLDER)
            tasks = []
            
            for advocate in advocates:
                advocate_href = advocate.get("href")
                if advocate_href:
                    print(f"Fetching details for advocate {advocate.get('name')} from {advocate_href}...")
                    advocate_data = await fetch_advocate_details(session, advocate_href)
                    
                    if advocate_data:
                        # Look for image in the advocate's details
                        image_url = advocate_data.get("images", {}).get("href")
                        image_path = None
                        
                        if image_url:
                            # Save image and get the image path
                            image_name = f"{advocate_data['name'].replace(' ', '_')}.jpg"
                            image_path = await fetch_and_save_image(session, image_url, attorney_folder, image_name)
                        
                        # Save the advocate's JSON with image information
                        await update_attorney_json(advocate_data, attorney_folder, image_path)
            
        else:
            print(f"No advocates found for case {case.get('id')}.")
    else:
        print(f"No href found for case {case.get('id')}. Skipping...")

async def process_resolved_cases():
    """
    Process each case in the resolved folder to update with advocates' information.
    """
    async with aiohttp.ClientSession() as session:
        tasks = []
        # Walk through each subfolder inside RESOLVED_FOLDER
        for case_folder in os.listdir(RESOLVED_FOLDER):
            case_folder_path = os.path.join(RESOLVED_FOLDER, case_folder)
            if os.path.isdir(case_folder_path):  # Ensure it's a directory (case folder)
                for case_file in os.listdir(case_folder_path):
                    if case_file.endswith(".json"):  # Process only JSON files
                        tasks.append(process_advocates(case_folder_path, case_file, session))

        await asyncio.gather(*tasks)

async def main():
    """
    Main function to orchestrate updating resolved cases with attorney images and info.
    """
    print("Updating resolved cases with advocates' information and images...")
    start_time = time.time()
    await process_resolved_cases()
    print(f"All resolved cases updated. Time taken: {time.time() - start_time} seconds.")

if __name__ == "__main__":
    # Run the asyncio event loop to update all cases
    asyncio.run(main())
