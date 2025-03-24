from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import mimetypes
import uuid
import threading
import time
from werkzeug.utils import secure_filename
from urllib.parse import unquote
from flask import Response

app = Flask(__name__, static_folder='../frontend/dist')
CORS(app, resources={r"/*": {
    "origins": ["http://localhost:3000", "https://image-tagging-frontend.vercel.app"],
    "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    "allow_headers": ["Content-Type", "Authorization"]
}})
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response
# Image extensions to filter by
IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.tiff']

load_dotenv()

# Configure S3 client with resource pooling
s3_session = boto3.Session(
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name=os.environ.get('AWS_REGION', 'us-east-1')
)
s3_client = s3_session.client('s3')
s3_resource = s3_session.resource('s3')
S3_BUCKET = os.environ.get('S3_BUCKET_NAME')
UNCATEGORIZED_FOLDER = "Uncategorized_Images/"

# Cache for directory listings
directory_cache = {}
cache_timeout = 300  # 5 minutes

def clear_expired_cache():
    """Clear expired cache entries"""
    current_time = time.time()
    expired_keys = [k for k, v in directory_cache.items() if current_time - v['timestamp'] > cache_timeout]
    for key in expired_keys:
        directory_cache.pop(key, None)

@app.route("/")
def index():
    return jsonify({
        "message": "Welcome to the API!",
        "status": "running"
    })

@app.route('/api/list-directories', methods=['GET'])
def list_directories():
    """List S3 directories (prefixes) with caching"""
    cache_key = "root_directories"
    
    # Check cache first
    if cache_key in directory_cache:
        cache_entry = directory_cache[cache_key]
        if time.time() - cache_entry['timestamp'] < cache_timeout:
            return jsonify({"directories": cache_entry['data']})
    
    try:
        # List the top-level "directories" (prefixes) in the S3 bucket
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Delimiter='/'
        )
        
        directories = []
        
        # Add the root directory
        directories.append({"path": "", "name": "Root"})
        
        # Add the common prefixes (folders)
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                prefix_name = prefix['Prefix'].rstrip('/')
                directories.append({
                    "path": prefix['Prefix'],
                    "name": prefix_name
                })
        
        # Update cache
        directory_cache[cache_key] = {
            'data': directories,
            'timestamp': time.time()
        }
        
        return jsonify({"directories": directories})
    except Exception as e:
        print("Error in list-directories:", str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/api/list-subdirectories', methods=['POST'])
def list_subdirectories():
    """List subdirectories (prefixes) within a given S3 prefix with caching"""
    data = request.json
    parent_prefix = data.get('directory', '')
    
    # Create cache key
    cache_key = f"subdirs_{parent_prefix}"
    
    # Check cache first
    if cache_key in directory_cache:
        cache_entry = directory_cache[cache_key]
        if time.time() - cache_entry['timestamp'] < cache_timeout:
            return jsonify({"subdirectories": cache_entry['data']})
    
    try:
        # Ensure the prefix ends with a slash if it's not empty
        if parent_prefix and not parent_prefix.endswith('/'):
            parent_prefix += '/'
            
        # Use pagination to handle large directories
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix=parent_prefix,
            Delimiter='/'
        )
        
        subdirs = []
        
        # Process all pages
        for page in pages:
            # Process common prefixes (folders)
            if 'CommonPrefixes' in page:
                for prefix in page['CommonPrefixes']:
                    # Skip the parent prefix itself
                    if prefix['Prefix'] == parent_prefix:
                        continue
                        
                    # Extract the folder name from the prefix
                    folder_name = prefix['Prefix'][len(parent_prefix):].rstrip('/')
                    
                    subdirs.append({
                        "path": prefix['Prefix'],
                        "name": folder_name
                    })
        
        # Update cache
        directory_cache[cache_key] = {
            'data': subdirs,
            'timestamp': time.time()
        }
        
        return jsonify({"subdirectories": subdirs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/list-images', methods=['POST'])
def list_images():
    """List all images in a specified S3 prefix with pagination"""
    data = request.json
    folder_path = data.get('folderPath', '')
    page_size = data.get('pageSize', 100)
    page = data.get('page', 1)
    
    try:
        # Ensure the prefix ends with a slash if it's not empty
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
            
        # Use pagination to handle large directories
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix=folder_path
        )
        
        image_files = []
        
        # Process all pages
        for page_response in pages:
            # Process objects (files)
            if 'Contents' in page_response:
                for obj in page_response['Contents']:
                    # Skip "directory" objects (ending with /)
                    if obj['Key'].endswith('/'):
                        continue
                        
                    # Check if the file is an image
                    file_ext = os.path.splitext(obj['Key'])[1].lower()
                    if file_ext in IMAGE_EXTENSIONS:
                        # Extract the filename from the key
                        filename = obj['Key'][len(folder_path):] if folder_path else obj['Key']
                        image_files.append({
                            "filename": filename,
                            "key": obj['Key'],
                            "lastModified": obj['LastModified'].isoformat(),
                            "size": obj['Size']
                        })
        
        # Total count of images
        total_count = len(image_files)
        
        # Calculate pagination
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        paginated_images = image_files[start_idx:end_idx]
        
        return jsonify({
            "images": paginated_images,
            "totalCount": total_count,
            "folderPath": folder_path,
            "currentPage": page,
            "pageSize": page_size,
            "totalPages": (total_count + page_size - 1) // page_size
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/image/<path:key>')
def get_image(key):
    """Serve an image file from S3"""
    try:
        # Decode the URL-encoded key
        decoded_key = unquote(key)
        
        # Get the object from S3
        response = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key=decoded_key
        )
        
        # Get the file content
        file_content = response['Body'].read()
        
        # Determine content type
        content_type = response.get('ContentType', 'application/octet-stream')
        
        # Return the file
        return Response(
            file_content,
            mimetype=content_type
        )
    except Exception as e:
        app.logger.error(f"Error serving image {key}: {str(e)}")
        return jsonify({"error": str(e)}), 500
    
@app.route('/api/save-categorized', methods=['POST'])
def save_categorized():
    """Move categorized images to new folders in S3 based on categories"""
    data = request.json
    source_folder = data.get('sourceFolder', '')
    categorized_images = data.get('categorizedImages', [])
    
    if not categorized_images:
        return jsonify({"error": "No categorized images provided"}), 400
    
    try:
        # Ensure the source folder ends with a slash if it's not empty
        if source_folder and not source_folder.endswith('/'):
            source_folder += '/'

        folder_name = os.path.basename(source_folder.rstrip('/')) if source_folder else "unnamed"
            
        # Create destination parent folder in S3
        # dest_parent = f"{folder_name}_categorized/"
        dest_parent = "CategorizedFiles/"
        
        # Process each image
        results = []
        bucket = s3_resource.Bucket(S3_BUCKET)
        
        for img_data in categorized_images:
            if 'filename' not in img_data or 'category' not in img_data:
                continue
                
            filename = img_data['filename']
            category = img_data['category']
            
            # Source and destination paths in S3
            source_key = os.path.join(source_folder, filename).replace('\\', '/')
            name, ext = os.path.splitext(filename)
            new_filename = f"{name}_{category}{ext}"
            dest_key = os.path.join(dest_parent, category, new_filename).replace('\\', '/')
            
            try:
                # Copy the object within S3
                bucket.copy(
                    {'Bucket': S3_BUCKET, 'Key': source_key},
                    dest_key
                )
                
                # Delete the original file
                s3_client.delete_object(
                    Bucket=S3_BUCKET,
                    Key=source_key
                )
                
                results.append({
                    "original": filename,
                    "renamed": new_filename,
                    "category": category,
                    "success": True
                })
            except Exception as e:
                results.append({
                    "original": filename,
                    "error": str(e),
                    "success": False
                })
        
        # Clear the cache for this directory
        cache_key = f"subdirs_{source_folder}"
        if cache_key in directory_cache:
            directory_cache.pop(cache_key)
        
        return jsonify({
            "results": results,
            "categorizedCount": len([r for r in results if r.get('success', False)]),
            "destinationFolder": dest_parent
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/upload-images', methods=['POST'])
def upload_images():
    """Upload images to the Uncategorized_Images folder in S3"""
    if 'files' not in request.files:
        return jsonify({"error": "No files provided"}), 400
    
    uploaded_files = request.files.getlist('files')
    
    if not uploaded_files or uploaded_files[0].filename == '':
        return jsonify({"error": "No files selected"}), 400
    
    results = []
    
    for file in uploaded_files:
        if file and allowed_file(file.filename):
            try:
                # Secure the filename
                filename = secure_filename(file.filename)
                
                # Generate a unique filename to avoid collisions
                unique_filename = f"{uuid.uuid4().hex}_{filename}"
                
                # Set the S3 key for the uncategorized folder
                s3_key = f"{UNCATEGORIZED_FOLDER}{unique_filename}"
                
                # Determine the content type
                content_type = mimetypes.guess_type(filename)[0] or 'application/octet-stream'
                
                # Upload the file to S3
                s3_client.upload_fileobj(
                    file,
                    S3_BUCKET,
                    s3_key,
                    ExtraArgs={
                        'ContentType': content_type
                    }
                )
                
                results.append({
                    "originalName": filename,
                    "storedName": unique_filename,
                    "s3Key": s3_key,
                    "success": True
                })
            except Exception as e:
                results.append({
                    "originalName": file.filename,
                    "error": str(e),
                    "success": False
                })
        else:
            results.append({
                "originalName": file.filename,
                "error": "File type not allowed",
                "success": False
            })
    
    # Clear the cache for the uncategorized folder
    cache_key = f"subdirs_{UNCATEGORIZED_FOLDER}"
    if cache_key in directory_cache:
        directory_cache.pop(cache_key)
    
    return jsonify({
        "results": results,
        "uploadedCount": len([r for r in results if r.get('success', False)]),
        "destinationFolder": UNCATEGORIZED_FOLDER
    })

def allowed_file(filename):
    """Check if the file extension is allowed"""
    return '.' in filename and \
           os.path.splitext(filename)[1].lower() in IMAGE_EXTENSIONS

# Utility function to check if a key exists in S3
def key_exists(key):
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except ClientError:
        return False

# Serve React app in production
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve(path):
    if path != "" and os.path.exists(app.static_folder + '/' + path):
        return send_from_directory(app.static_folder, path)
    else:
        return send_from_directory(app.static_folder, 'index.html')

# Start a background thread to clear expired cache entries
def cache_cleaner():
    while True:
        clear_expired_cache()
        time.sleep(60)  # Run every minute

if __name__ == '__main__':
    # Start cache cleaner in background
    cache_thread = threading.Thread(target=cache_cleaner, daemon=True)
    cache_thread.start()
    
    app.run(host="0.0.0.0", port=8080)