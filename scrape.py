import os
import re
import logging
from utils.utils import (
    load_already_processed_files,
    url_to_sanitized_base_no_ext,
    fetch_and_save_html_md,
    check_chunks_exist_for_url,
    create_and_save_chunks,
    iterate_csv_rows,
)
from utils.ai_utils import (
    chunk_prompt,
    CHAT_CLIENT,
    SYSTEM_PROMPT,
)
from config import (
    HTML_OUTPUT_DIR,
    MD_OUTPUT_DIR,
    CHUNKS_OUTPUT_DIR,
    CSV_FILENAME,
    PROCESS_ES,
    MAX_CHUNK_SIZE,
    PUBLIC_SITE_URL,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
    handlers=[
        logging.FileHandler("scraper.log"),  # Log to a file
        logging.StreamHandler(),  # Log to console
    ],
)


def scrape_and_save_raw_content():
    """
    Iterates over the CSV, scrapes URLs, and saves HTML and Markdown files.
    """
    logging.info("--- Starting HTML and Markdown Generation ---")
    os.makedirs(HTML_OUTPUT_DIR, exist_ok=True)
    os.makedirs(MD_OUTPUT_DIR, exist_ok=True)

    processed_base_files_set = load_already_processed_files(HTML_OUTPUT_DIR)
    logging.info(
        f"Found {len(processed_base_files_set)} already processed HTML files in '{HTML_OUTPUT_DIR}'."
    )

    new_files_scraped_count = 0
    skipped_html_md_count = 0
    processed_urls_count = 0

    rows, total_csv_rows = iterate_csv_rows(CSV_FILENAME, "HTML/MD generation")
    if rows is None:  # Check if helper indicated an error or no data
        return

    for i, row in enumerate(rows, 1):
        url = row.get("Full URL", "").strip()
        title = row.get("Page Title", "No Title Provided").strip()

        if not url:
            logging.warning(f"({i}/{total_csv_rows}) Skipping row with empty URL.")
            skipped_html_md_count += 1
            continue

        processed_urls_count += 1
        logging.info(f"({i}/{total_csv_rows}) Processing for HTML/MD: {title}, {url}")

        if (
            url == PUBLIC_SITE_URL + "/"
        ):  # PUBLIC_SITE_URL might be better here if it can change
            logging.info(f"  -> Skipping homepage URL: {url}")
            skipped_html_md_count += 1
            continue
        if not PROCESS_ES and "/es/" in url:
            logging.info(f"  -> Skipping Spanish URL (not processing HTML/MD): {url}")
            skipped_html_md_count += 1
            continue

        sanitized_base_name = url_to_sanitized_base_no_ext(url)
        if sanitized_base_name in processed_base_files_set:
            logging.info(f"  -> Skipping (HTML/MD already exists): {url}")
            skipped_html_md_count += 1
            continue

        success = fetch_and_save_html_md(url, title)  # Synchronous call
        if success:
            processed_base_files_set.add(sanitized_base_name)
            new_files_scraped_count += 1
        else:
            logging.error(f"  -> Failed to process and save HTML/MD for {url}")
            # This URL will be skipped for chunking if MD doesn't exist

    logging.info("--- HTML/Markdown Generation Summary ---")
    logging.info(f"Total URLs from CSV: {total_csv_rows}")
    logging.info(f"URLs attempted for HTML/MD processing: {processed_urls_count}")
    logging.info(f"Newly scraped and saved HTML/MD files: {new_files_scraped_count}")
    logging.info(
        f"Skipped HTML/MD (already existing, invalid, or skipped type): {skipped_html_md_count}"
    )
    total_html_files = (
        len(os.listdir(HTML_OUTPUT_DIR)) if os.path.exists(HTML_OUTPUT_DIR) else 0
    )
    total_md_files = (
        len(os.listdir(MD_OUTPUT_DIR)) if os.path.exists(MD_OUTPUT_DIR) else 0
    )
    logging.info(
        f"Total HTML files currently in '{HTML_OUTPUT_DIR}': {total_html_files}"
    )
    logging.info(
        f"Total Markdown files currently in '{MD_OUTPUT_DIR}': {total_md_files}"
    )


# Generates and saves chunks
def generate_and_save_chunks():
    """
    Iterates over the CSV, reads corresponding Markdown files,
    generates chunks, and saves them if they don't already exist.
    """
    logging.info("--- Starting Chunk Generation ---")
    os.makedirs(CHUNKS_OUTPUT_DIR, exist_ok=True)

    new_files_chunked_count = 0
    skipped_chunking_count = 0
    processed_urls_for_chunks_count = 0
    md_files_not_found_count = 0

    rows, total_csv_rows = iterate_csv_rows(CSV_FILENAME, "chunk generation")
    if rows is None:  # Check if helper indicated an error or no data
        return

    for i, row in enumerate(rows, 1):
        url = row.get("Full URL", "").strip()
        title = row.get("Page Title", "No Title Provided").strip()

        if not url:
            logging.warning(
                f"({i}/{total_csv_rows}) Skipping row with empty URL for chunking."
            )
            skipped_chunking_count += 1
            continue

        processed_urls_for_chunks_count += 1
        logging.info(f"({i}/{total_csv_rows}) Processing for Chunks: {title}, {url}")

        if url == PUBLIC_SITE_URL + "/":
            logging.info(f"  -> Skipping homepage URL for chunking: {url}")
            skipped_chunking_count += 1
            continue
        if not PROCESS_ES and "/es/" in url:
            logging.info(f"  -> Skipping Spanish URL (not processing chunks): {url}")
            skipped_chunking_count += 1
            continue

        sanitized_base_name = url_to_sanitized_base_no_ext(url)

        if check_chunks_exist_for_url(url, CHUNKS_OUTPUT_DIR):
            logging.info(f"  -> Skipping (chunks already exist for this URL): {url}")
            skipped_chunking_count += 1
            continue

        md_filename = sanitized_base_name + ".md"
        md_path = os.path.join(MD_OUTPUT_DIR, md_filename)

        if not os.path.exists(md_path):
            logging.warning(
                f"  -> Markdown file not found, cannot create chunks: {md_path}"
            )
            md_files_not_found_count += 1
            skipped_chunking_count += 1
            continue

        try:
            with open(md_path, "r", encoding="utf-8") as f_md:
                md_content = f_md.read()
        except Exception as e:
            logging.error(f"  -> Error reading Markdown file {md_path}: {e}")
            skipped_chunking_count += 1
            continue

        if not md_content.strip():
            logging.warning(
                f"  -> Markdown file is empty, skipping chunk generation: {md_path}"
            )
            skipped_chunking_count += 1
            continue

        logging.info(f"  -> Generating chunks for: {md_path}")
        prompt_content = chunk_prompt(url, title, md_content)

        create_and_save_chunks(
            prompt_content,
            CHAT_CLIENT,
            SYSTEM_PROMPT,
            sanitized_base_name,
        )
        break  # For testing, break after first chunk generation

    logging.info("--- Chunk Generation Summary ---")
    logging.info(f"Total URLs from CSV: {total_csv_rows}")
    logging.info(f"URLs attempted for chunking: {processed_urls_for_chunks_count}")
    logging.info(
        f"Markdown files for which new chunks were generated: {new_files_chunked_count}"
    )
    logging.info(
        f"Skipped chunking (already existing, MD not found, error, or no chunks produced): {skipped_chunking_count}"
    )
    logging.info(
        f"Markdown files not found (preventing chunking): {md_files_not_found_count}"
    )
    total_chunk_files = (
        len(os.listdir(CHUNKS_OUTPUT_DIR)) if os.path.exists(CHUNKS_OUTPUT_DIR) else 0
    )
    logging.info(
        f"Total chunk files currently in '{CHUNKS_OUTPUT_DIR}': {total_chunk_files}"
    )


def reformat_chunks():
    retries = 0
    max_chars = (
        MAX_CHUNK_SIZE + 1
    )  # Initialize max_chars to be greater than MAX_CHUNK_SIZE to enter the loop
    total_chunks = 0

    # Retry loop: This loop will continue as long as any chunk exceeds MAX_CHUNK_SIZE
    # and the number of retries is less than 25.
    # This is to handle cases where re-chunking might still produce oversized chunks,
    # allowing for multiple attempts to fix them.
    while max_chars > MAX_CHUNK_SIZE and retries < 25:
        retries_msg = f" (attempt {retries + 1}/25)" if retries > 0 else ""
        logging.info(f"Running chunk formatting{retries_msg}...")
        max_chars = 0  # Reset max_chars for the current iteration
        total_chunks = 0
        for filename in os.listdir(CHUNKS_OUTPUT_DIR):

            if filename.endswith(".md"):
                file_path = os.path.join(CHUNKS_OUTPUT_DIR, filename)
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()

                # Remove prompt helper text used during AI model generation
                # These markers are added by the AI and are not part of the actual content.
                if "--- Begin Chunk ---" in content:
                    content = content.replace("--- Begin Chunk ---", "")
                if "--- End Chunk ---" in content:
                    content = content.replace("--- End Chunk ---", "")
                if "--- Begin Chunk Metadata ---" in content:
                    content = content.replace("--- Begin Chunk Metadata ---", "")
                if "--- End Chunk Metadata ---" in content:
                    content = content.replace("--- End Chunk Metadata ---", "")
                # Further cleanup of metadata prefixes
                if "Full URL: " in content:
                    content = content.replace("Full URL: ", "")
                if "Title: " in content:
                    content = content.replace("Title: ", "")
                if "Example Query One: " in content:
                    content = content.replace("Example Query One: ", "")
                if "Example Query Two: " in content:
                    content = content.replace("Example Query Two: ", "")

                # count total characters in the content
                total_chars = len(content)
                if total_chars > max_chars:
                    max_chars = (
                        total_chars  # Update max_chars if current chunk is larger
                    )

                # Condition for reprocessing: If a chunk's character count is greater than or equal to MAX_CHUNK_SIZE.
                if total_chars >= MAX_CHUNK_SIZE:
                    url = ""  # Initialize url for the chunk_prompt
                    title = ""  # Initialize title for the chunk_prompt
                    # TODO: Consider fetching the original URL and Title if needed for re-chunking context,
                    # currently, they are passed as empty strings. This might affect re-chunking quality.
                    logging.info(
                        f"  -> Chunk {filename} is too long ({total_chars} chars), re-chunking."
                    )
                    # The content passed to chunk_prompt here is the *current* content of the oversized chunk,
                    # not the original full markdown document.
                    prompt_content = chunk_prompt(url, title, content)
                    sanitized_base_name = os.path.splitext(filename)[0]
                    # Remove _chunk_X suffix if it exists to avoid base_name_chunk_X_chunk_Y pattern
                    # when create_and_save_chunks adds its own _chunk_idx.
                    sanitized_base_name = re.sub(
                        r"_chunk_\\d+$", "", sanitized_base_name
                    )

                    create_and_save_chunks(
                        prompt_content,
                        CHAT_CLIENT,
                        SYSTEM_PROMPT,
                        sanitized_base_name,
                    )
                    logging.info(
                        f"  -> Re-chunked {filename} due to excessive length: {total_chars} characters."
                    )
                    # delete original file after re-chunking to avoid processing it again
                    os.remove(file_path)
                    logging.info(f"  -> Deleted original over-sized file: {file_path}")
                else:
                    total_chunks += 1

                    # Condition for renaming: If filename starts with the sanitized usa.gov prefix.
                    # This is a cosmetic step to shorten filenames.
                    # TODO: Consider moving this renaming logic to the initial chunk creation
                    # in `create_and_save_chunks` to avoid this separate step.
                    if filename.startswith("https_www_usa_DOT_gov_SLASH_"):
                        old_filepath = os.path.join(
                            CHUNKS_OUTPUT_DIR, filename
                        )  # Corrected: file_path was assigned to old_filepath
                        new_filename = filename.replace(
                            "https_www_usa_DOT_gov_SLASH_", ""
                        ).strip()
                        new_file_path = os.path.join(CHUNKS_OUTPUT_DIR, new_filename)
                        os.rename(old_filepath, new_file_path)  # Safely rename
                        logging.info(f"  -> Renamed {filename} to {new_filename}")
                        file_path = new_file_path  # Update file_path to the new path for writing

                    # Write back the reformatted content (after removing helper text and potentially renaming)
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(content)

                    logging.info(f"Reformatted chunk file: {file_path}")

            retries += 1  # Increment retry counter for each file processed in the directory (consider if this should be per oversized chunk) # noqa: E501


def main():
    """
    Main function to drive the CSV reading, scraping, HTML/MD saving, and chunk generation.
    """
    # Ensure global output directories exist (also done at top level and within functions)
    os.makedirs(HTML_OUTPUT_DIR, exist_ok=True)
    os.makedirs(MD_OUTPUT_DIR, exist_ok=True)
    os.makedirs(CHUNKS_OUTPUT_DIR, exist_ok=True)

    scrape_and_save_raw_content()
    generate_and_save_chunks()
    reformat_chunks()


if __name__ == "__main__":
    main()
