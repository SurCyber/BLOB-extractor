import sqlite3
import os
import magic
from tqdm import tqdm
import csv

def get_file_extension(mime_type):
    mapping = {
        # Images
        'image/png': 'png',
        'image/jpeg': 'jpeg',
        'image/bmp': 'bmp',
        'image/svg+xml': 'svg',
        'image/vnd.adobe.photoshop': 'psd',

        # Videos
        'video/mp4': 'mp4',
        'video/x-msvideo': 'avi',
        'video/x-matroska': 'mkv',

        # Audio
        'audio/mpeg': 'mp3',
        'audio/wav': 'wav',
        'audio/ogg': 'ogg',
        'audio/aac': 'aac',
        'audio/flac': 'flac',

        # Documents
        'application/pdf': 'pdf',
        'application/msword': 'doc',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'docx',
        'application/vnd.ms-excel': 'xls',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'xlsx',
        'application/vnd.ms-powerpoint': 'ppt',
        'application/vnd.openxmlformats-officedocument.presentationml.presentation': 'pptx',
        'text/plain': 'txt',

        # Archives
        'application/zip': 'zip',
        'application/vnd.rar': 'rar',
        'application/x-7z-compressed': '7z',
        'application/gzip': 'gz',
        'application/x-tar': 'tar',

        # Executables
        'application/x-msdownload': 'exe',
        'application/x-ms-installer': 'msi',
        'application/x-executable': 'elf',
        'application/x-elf': 'elf',

        # Code/Markup
        'text/html': 'html',
        'application/json': 'json',
        'text/x-python': 'py',
        'application/javascript': 'js',
        'application/xml': 'xml',
        'text/xml': 'xml',
        'application/x-yaml': 'yaml',
        'text/yaml': 'yaml',

        # eBooks
        'application/epub+zip': 'epub',
        'application/x-mobipocket-ebook': 'mobi',

        # Design
        'application/postscript': 'ai'
    }
    return mapping.get(mime_type, None)

def list_tables(cursor):
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    return [row[0] for row in cursor.fetchall()]

def get_columns(cursor, table):
    cursor.execute(f"PRAGMA table_info({table});")
    return cursor.fetchall()

def extract_blobs(db_file, output_path):
    try:
        os.makedirs(output_path, exist_ok=True)  # Ensure base output path exists
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        tables = list_tables(cursor)
        if not tables:
            print("❌ No tables found in the database.")
            return

        print("\n📋 Step 1: Select a table:")
        for idx, name in enumerate(tables, start=1):
            print(f"{idx}. {name}")
        t_idx = int(input("Enter table number: ")) - 1
        table = tables[t_idx]

        columns = get_columns(cursor, table)
        blob_cols = [col[1] for col in columns if col[2].upper() == 'BLOB']
        col_names = [col[1] for col in columns]

        if not blob_cols:
            print("❌ No BLOB column found.")
            return

        print("\n🧊 Select BLOB column:")
        for i, col in enumerate(blob_cols):
            print(f"{i+1}. {col}")
        b_idx = int(input("Enter BLOB column number: ")) - 1
        blob_col = blob_cols[b_idx]

        print("\n🔢 Select ID column:")
        for i, col in enumerate(col_names):
            print(f"{i+1}. {col}")
        id_idx = int(input("Enter ID column number: ")) - 1
        id_col = col_names[id_idx]

        cursor.execute(f"SELECT {id_col}, {blob_col} FROM {table}")
        rows = cursor.fetchall()

        folders = {
            'images': None,
            'videos': None,
            'documents': None,
            'archives': None,
            'audio': None,
            'executables': None,
            'code': None,
            'ebooks': None,
            'unknown': None
        }

        mime = magic.Magic(mime=True)
        count = {k: 0 for k in folders}

        log_csv = os.path.join(output_path, 'extracted_files.csv')
        with open(log_csv, 'w', newline='') as logfile:
            writer = csv.writer(logfile)
            writer.writerow(['ID', 'MIME Type', 'Extension', 'Saved Path'])

            for row_id, blob_data in tqdm(rows, desc="Extracting"):
                if blob_data is None:
                    continue

                mime_type = mime.from_buffer(blob_data)
                ext = get_file_extension(mime_type)

                category = 'unknown'
                if ext in ['png', 'jpeg', 'bmp', 'svg', 'psd']:
                    category = 'images'
                elif ext in ['mp4', 'avi', 'mkv']:
                    category = 'videos'
                elif ext in ['pdf', 'doc', 'docx', 'xls', 'xlsx', 'ppt', 'pptx', 'txt']:
                    category = 'documents'
                elif ext in ['zip', 'rar', '7z', 'gz', 'tar']:
                    category = 'archives'
                elif ext in ['mp3', 'wav', 'ogg', 'aac', 'flac']:
                    category = 'audio'
                elif ext in ['exe', 'msi', 'elf']:
                    category = 'executables'
                elif ext in ['html', 'json', 'py', 'js', 'xml', 'yaml']:
                    category = 'code'
                elif ext in ['epub', 'mobi']:
                    category = 'ebooks'
                else:
                    ext = 'bin'

                if folders[category] is None:
                    folders[category] = os.path.join(output_path, category)
                    os.makedirs(folders[category], exist_ok=True)

                filename = f"{row_id}.{ext}"
                file_path = os.path.join(folders[category], filename)

                with open(file_path, 'wb') as f:
                    f.write(blob_data)

                count[category] += 1
                writer.writerow([row_id, mime_type, ext, file_path])

        print("\n✅ Extraction Summary:")
        for k, v in count.items():
            print(f"{k.capitalize():<12}: {v}")

    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    print("📦 Enhanced SQLite BLOB Extractor")
    db_file = input("Enter path to SQLite .db file: ").strip()
    output_path = input("Enter output directory: ").strip()
    extract_blobs(db_file, output_path)
