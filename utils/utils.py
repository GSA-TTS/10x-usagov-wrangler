import requests
import csv
import os
import re
from bs4 import BeautifulSoup

from markdownify import markdownify as md

from utils.ai_utils import chat_completion

from config import (
    HTML_OUTPUT_DIR,
    MD_OUTPUT_DIR,
    CHUNKS_OUTPUT_DIR,
    USER_AGENT,
    PUBLIC_SITE_URL,
)

from dotenv import load_dotenv

load_dotenv()


def iterate_csv_rows(csv_filename: str, process_stage: str):
    """Helper function to read and iterate over rows in the CSV file."""
    try:
        with open(csv_filename, mode="r", encoding="utf-8-sig") as csvfile:
            reader = csv.DictReader(csvfile)
            if "Full URL" not in reader.fieldnames:
                print(
                    f"Error: 'Full URL' column not found in '{csv_filename}'. Cannot proceed with {process_stage}."
                )
                return None, 0  # Indicate error

            rows = list(reader)
            total_rows = len(rows)

            if total_rows == 0:
                print(f"No URLs found in '{csv_filename}' for {process_stage}.")
                return None, 0  # Indicate no data

            print(
                f"Found {total_rows} URLs in '{csv_filename}'. Starting {process_stage} processing...\n"
            )
            return rows, total_rows
    except FileNotFoundError:
        print(
            f"Error: The CSV file '{csv_filename}' was not found for {process_stage}."
        )
        return None, 0  # Indicate error
    except Exception as e:
        print(
            f"Error reading or parsing CSV file '{csv_filename}' for {process_stage}: {e}"
        )
        return None, 0  # Indicate error


def sanitize_filename(name):
    """
    Sanitizes a string to be used as a filename.
    Removes or replaces characters that are problematic in filenames.
    """
    name = re.sub(
        r"[^\w\s-]", "", name
    )  # Remove non-alphanumeric, non-space, non-hyphen
    name = re.sub(r"[-\s]+", "-", name).strip(
        "-_"
    )  # Replace spaces/hyphens with single hyphen
    return name


def url_to_sanitized_base_no_ext(url: str) -> str:
    """
    Converts a URL to a sanitized string suitable for use as a base filename (without extension).
    This is an internal helper function.
    """
    filename = url

    # Order of these initial prefix replacements matters
    if filename.startswith("https://www."):
        filename = "https_www_" + filename[len("https://www.") :]
    elif filename.startswith("http://www."):
        filename = "http_www_" + filename[len("http://www.") :]
    elif filename.startswith("https://"):
        filename = "https_" + filename[len("https://") :]
    elif filename.startswith("http://"):
        filename = "http_" + filename[len("http://") :]
    # Add other schemes like ftp_ if necessary

    # Define character replacements for filesystem safety and reversibility
    # Using distinct placeholders ensures reliable reversal.
    replacements = {
        "/": "_SLASH_",
        "?": "_QUERY_",
        "=": "_EQ_",
        "&": "_AMP_",
        "%": "_PCT_",
        ".": "_DOT_",
        ":": "_COLON_",  # Handles colons in ports or other parts of URL
        "\\": "_BACKSLASH_",
        "*": "_STAR_",
        '"': "_QUOTE_",
        "<": "_LT_",
        ">": "_GT_",
        "|": "_PIPE_",
    }
    for char, replacement_token in replacements.items():
        filename = filename.replace(char, replacement_token)

    # Note: OS filename length limits (e.g., 255 bytes/chars on many systems)
    # could be an issue for extremely long URLs. For typical usa.gov URLs,
    # this should not be a problem. If it were, a hashing mechanism for
    # overly long names might be needed, which would complicate reversal.

    return filename


def _sanitized_base_no_ext_to_url(base_filename: str) -> str:
    """
    Converts a sanitized base filename (without extension) back to its original URL.
    This is an internal helper function.
    """
    url = base_filename

    # Define tokens to be replaced back to original characters.
    # Order can matter if tokens could be substrings of others or if replacement creates new tokens.
    # These tokens are designed to be distinct.
    reverse_replacements = {
        "_SLASH_": "/",
        "_QUERY_": "?",
        "_EQ_": "=",
        "_AMP_": "&",
        "_PCT_": "%",
        "_DOT_": ".",
        "_COLON_": ":",
        "_BACKSLASH_": "\\",
        "_STAR_": "*",
        "_QUOTE_": '"',
        "_LT_": "<",
        "_GT_": ">",
        "_PIPE_": "|",
    }

    for token, original_char in reverse_replacements.items():
        url = url.replace(token, original_char)

    # Restore original URL scheme. Order is important: check longer prefixes first.
    if url.startswith("https_www_"):
        url = "https://www." + url[len("https_www_") :]
    elif url.startswith("http_www_"):
        url = "http://www." + url[len("http_www_") :]
    elif url.startswith("https_"):
        url = "https://" + url[len("https_") :]
    elif url.startswith("http_"):
        url = "http://" + url[len("http_") :]
    else:
        # This case implies the original URL might not have had a recognized scheme prefix,
        # or the sanitization/desanitization logic for schemes needs adjustment.
        # Given the input (usa.gov), URLs are expected to be standard http/https.
        print(
            f"Warning: Could not restore original scheme for base filename '{base_filename}'. Current URL: {url}"
        )

    return url


def url_to_filename(url: str) -> str:
    """
    Converts a URL to a filesystem-safe filename, including the .html extension.
    This function is intended for direct use.
    """
    sanitized_base = url_to_sanitized_base_no_ext(url)
    return sanitized_base + ".html"


def filename_to_url(filename_with_ext: str) -> str:
    """
    Converts a filesystem-safe filename (e.g., 'example_DOT_com_SLASH_page.html')
    back to the original URL.
    This function is intended for direct use.
    """
    if not filename_with_ext.endswith(".html"):
        error_msg = f"Error: Filename '{filename_with_ext}' must end with '.html' for valid reversal."
        print(error_msg)
        return error_msg  # Or raise ValueError(error_msg)

    base_filename = filename_with_ext[: -len(".html")]
    return _sanitized_base_no_ext_to_url(base_filename)


def load_already_processed_files(directory: str) -> set:
    """
    Scans the specified directory for .html files and returns a set of
    their base names (without the .html extension).
    """
    processed_files = set()
    if os.path.exists(directory) and os.path.isdir(directory):
        for f_name in os.listdir(directory):
            if f_name.endswith(".html"):
                processed_files.add(f_name[: -len(".html")])
    return processed_files


# NEW helper function to check if chunks exist for a URL
def check_chunks_exist_for_url(url: str, chunks_dir: str) -> bool:
    """Checks if chunk files already exist for a given URL."""
    base_name = url_to_sanitized_base_no_ext(url)
    # This checks for the existence of the first chunk file (e.g., basename_chunk_0.md)
    # as an indicator that chunks have been processed for this URL.
    potential_first_chunk = os.path.join(chunks_dir, f"{base_name}_chunk_0.md")
    return os.path.exists(potential_first_chunk)


# NEW function: Fetches and saves HTML and Markdown content
def fetch_and_save_html_md(url: str, title: str) -> bool:
    """
    Fetches HTML content for a URL, saves it, converts to Markdown, and saves Markdown.
    Returns True on success, False otherwise.
    """
    try:
        headers = {"User-Agent": USER_AGENT}
        response = requests.get(url, headers=headers, timeout=(10, 20))
        response.raise_for_status()  # Raises HTTPError for bad responses

        soup = BeautifulSoup(response.text, "html.parser")
        content_to_save = ""
        main_tag = soup.main
        if main_tag:
            content_to_save = main_tag.prettify()
        elif soup.body:
            content_to_save = soup.body.prettify()
        else:
            print(f"  -> No <main> or <body> tag found in {url}. Saving empty content.")

        target_filename_with_ext = url_to_filename(url)
        output_html_path = os.path.join(HTML_OUTPUT_DIR, target_filename_with_ext)
        with open(output_html_path, "w", encoding="utf-8") as f_html:
            f_html.write(content_to_save)
        print(f"  -> Saved HTML to: {output_html_path}")

        base_name, _ = os.path.splitext(target_filename_with_ext)
        md_filename = base_name + ".md"
        output_md_path = os.path.join(MD_OUTPUT_DIR, md_filename)
        md_content = md(content_to_save)
        with open(output_md_path, "w", encoding="utf-8") as f_md:
            f_md.write(md_content)
        print(f"  -> Saved Markdown to: {output_md_path}")
        return True

    except requests.exceptions.HTTPError as http_err:
        print(f"  -> HTTP error for {url}: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"  -> Connection error for {url}: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"  -> Timeout error for {url}: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"  -> General request error for {url}: {req_err}")
    except IOError as io_err:
        print(f"  -> File I/O error during HTML/MD saving for {url}: {io_err}")
    except Exception as e:
        print(
            f"  -> An unexpected error occurred while fetching/saving HTML/MD for {url}: {e}"
        )
    return False


def create_and_save_chunks(prompt_content, client, system_prompt, sanitized_base_name):
    chunks_string = chat_completion(prompt_content, client, system_prompt)

    chunks = chunks_string.split("--- End Chunk ---")
    md_url_pattern = r"(\]\()(/[^)]+)\)"  # Relative URLs starting with /

    actual_chunks_created_for_this_file = 0
    for chunk_idx, chunk_text in enumerate(chunks):
        # Ensure chunk is not just whitespace or empty after split
        processed_chunk_text = chunk_text.strip()
        if processed_chunk_text:
            chunk_filename = f"{sanitized_base_name}_chunk_{chunk_idx}.md"
            # Replace relative markdown links with absolute ones
            processed_chunk_text = re.sub(
                md_url_pattern,
                rf"]({PUBLIC_SITE_URL}\2)",
                processed_chunk_text,
            )

            output_chunk_path = os.path.join(CHUNKS_OUTPUT_DIR, chunk_filename)
            try:
                # Save chunk with the delimiter as per example output
                with open(output_chunk_path, "w", encoding="utf-8") as f_chunk:
                    f_chunk.write(processed_chunk_text + "\n--- End Chunk ---")
                print(f"  -> Saved chunk {chunk_idx} to: {output_chunk_path}")
                actual_chunks_created_for_this_file += 1
            except IOError as e:
                print(f"  -> Error saving chunk file {output_chunk_path}: {e}")
                raise

    return True if actual_chunks_created_for_this_file > 0 else False
