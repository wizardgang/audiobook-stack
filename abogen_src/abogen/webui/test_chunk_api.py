import sys
import os
import json
from pathlib import Path

# Add abogen to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from abogen.webui.app import create_app

def run_test():
    if len(sys.argv) < 2:
        print("Usage: python test_chunk_api.py <path_to_document>")
        print("Example: python test_chunk_api.py my_book.pdf")
        sys.exit(1)
        
    file_path = Path(sys.argv[1])
    if not file_path.exists():
        print(f"File not found: {file_path}")
        sys.exit(1)
        
    print(f"Initializing Abogen context...")
    app = create_app()
    app.testing = True
    client = app.test_client()
    
    # 1. Test /api/extract
    print(f"\n--- [1/2] Testing POST /api/extract with {file_path.name} ---")
    with open(file_path, "rb") as f:
        data = {"file": f}
        res = client.post("/api/extract", data=data, content_type="multipart/form-data")
        
    if res.status_code != 200:
        print(f"Extraction failed! Status: {res.status_code}\nData: {res.data.decode()}")
        return
        
    ext_data = json.loads(res.data)
    chapters = ext_data.get("chapters", [])
    print(f"Extraction successful! Found {len(chapters)} chapters.")
    
    if not chapters:
        print("No text was found in the document.")
        return
        
    # Select the first substantial chapter
    target_chapter = next((c for c in chapters if len(c.get("text", "").strip()) > 50), None)
    if not target_chapter:
        print("No Chapters with substantial text found.")
        return
        
    chapter_text = target_chapter["text"]
    print(f"Selected Chapter for chunking: '{target_chapter['title']}' ({len(chapter_text)} characters)")
    
    # 2. Test /api/chunk
    print(f"\n--- [2/2] Testing POST /api/chunk on logic parsing ---")
    
    # Sample the first 3000 chars to avoid wall-of-text
    sample_text = chapter_text[:3000] 
    
    chunk_payload = {
        "text": sample_text,
        "level": "sentence" # Using NLP semantic sentence breaking
    }
    
    c_res = client.post("/api/chunk", json=chunk_payload)
    if c_res.status_code != 200:
        print(f"Chunking failed! Status: {c_res.status_code}\nData: {c_res.data.decode()}")
        return
        
    chunk_data = json.loads(c_res.data)
    chunks = chunk_data.get("chunks", [])
    print(f"Chunking successful! Abogen's Spacy pipeline cleanly sliced the sample into {len(chunks)} NLP sentences:")
    
    for i, chunk in enumerate(chunks[:10]): # display the first 10
        cleaned_text = chunk.get('original_text', '').strip().replace('\n', ' ')
        print(f"  [{i+1}] {cleaned_text}")
        
    if len(chunks) > 10:
        print(f"  ... and {len(chunks)-10} more sentences parsed.")
        
    print("\nAPI Chunk test complete!")

if __name__ == "__main__":
    run_test()
