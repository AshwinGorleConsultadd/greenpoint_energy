"""
pdf_cutter.py

Purpose:
  Extract specific page range from a PDF and save to output folder.
  
Functions:
  - cut_pdf(pdf_path, from_page, to_page) -> str
"""

import os
from pathlib import Path
from typing import Tuple


def cut_pdf(pdf_path: str, from_page: int, to_page: int) -> str:
    """
    Extract pages from_page to to_page (inclusive) from a PDF and save to output folder.
    
    Args:
        pdf_path: Path to the input PDF file
        from_page: Starting page number (1-indexed, inclusive)
        to_page: Ending page number (1-indexed, inclusive)
    
    Returns:
        Path to the output PDF file
    
    Raises:
        FileNotFoundError: If input PDF doesn't exist
        ValueError: If page numbers are invalid
        Exception: If PDF processing fails
    """
    # Validate input file exists
    input_path = Path(pdf_path)
    if not input_path.exists():
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")
    
    # Validate page numbers
    if from_page < 1:
        raise ValueError(f"from_page must be >= 1, got {from_page}")
    if to_page < from_page:
        raise ValueError(f"to_page ({to_page}) must be >= from_page ({from_page})")
    
    # Try using PyPDF2 first (most common)
    try:
        import PyPDF2
        return _cut_with_pypdf2(pdf_path, from_page, to_page)
    except ImportError:
        try:
            # Try pypdf (newer version of PyPDF2)
            import pypdf
            return _cut_with_pypdf(pdf_path, from_page, to_page)
        except ImportError:
            raise ImportError(
                "Neither PyPDF2 nor pypdf is installed. "
                "Please install one of them: pip install pypdf"
            )


def _cut_with_pypdf2(pdf_path: str, from_page: int, to_page: int) -> str:
    """Extract pages using PyPDF2."""
    import PyPDF2
    
    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        
        # Validate pages exist
        total_pages = len(pdf_reader.pages)
        if from_page > total_pages:
            raise ValueError(f"from_page ({from_page}) exceeds total pages ({total_pages})")
        if to_page > total_pages:
            to_page = total_pages  # Clamp to maximum
        
        # Create new PDF with selected pages
        pdf_writer = PyPDF2.PdfWriter()
        
        # PyPDF2 uses 0-indexed pages
        for page_num in range(from_page - 1, to_page):
            pdf_writer.add_page(pdf_reader.pages[page_num])
        
        # Create output directory
        output_dir = Path(pdf_path).parent / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Generate output filename
        input_filename = Path(pdf_path).stem
        output_filename = f"{input_filename}_pages_{from_page}_to_{to_page}.pdf"
        output_path = output_dir / output_filename
        
        # Write to file
        with open(output_path, 'wb') as output_file:
            pdf_writer.write(output_file)
        
        return str(output_path)


def _cut_with_pypdf(pdf_path: str, from_page: int, to_page: int) -> str:
    """Extract pages using pypdf (newer library)."""
    import pypdf
    
    with open(pdf_path, 'rb') as file:
        pdf_reader = pypdf.PdfReader(file)
        
        # Validate pages exist
        total_pages = len(pdf_reader.pages)
        if from_page > total_pages:
            raise ValueError(f"from_page ({from_page}) exceeds total pages ({total_pages})")
        if to_page > total_pages:
            to_page = total_pages  # Clamp to maximum
        
        # Create new PDF with selected pages
        pdf_writer = pypdf.PdfWriter()
        
        # pypdf uses 0-indexed pages
        for page_num in range(from_page - 1, to_page):
            pdf_writer.add_page(pdf_reader.pages[page_num])
        
        # Create output directory
        output_dir = Path(pdf_path).parent / "output"
        output_dir.mkdir(exist_ok=True)
        
        # Generate output filename
        input_filename = Path(pdf_path).stem
        output_filename = f"{input_filename}_pages_{from_page}_to_{to_page}.pdf"
        output_path = output_dir / output_filename
        
        # Write to file
        with open(output_path, 'wb') as output_file:
            pdf_writer.write(output_file)
        
        return str(output_path)


# ---------- Example usage ----------
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 4:
        # Default behavior: cut first page (1 to 1) of input/top_400.pdf
        default_input = Path(__file__).parent / "input" / "top_400.pdf"
        pdf_path = str(default_input)
        from_page = 1
        to_page = 1
        print("No arguments provided. Using default:")
        print(f"  PDF: {pdf_path}")
        print(f"  Pages: {from_page} to {to_page}")
    else:
        pdf_path = sys.argv[1]
        from_page = int(sys.argv[2])
        to_page = int(sys.argv[3])
    
    try:
        output_path = cut_pdf(pdf_path, from_page, to_page)
        print(f"✅ Successfully extracted pages {from_page}-{to_page}")
        print(f"📄 Output saved to: {output_path}")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

