from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
import os
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from io import BytesIO
import mimetypes
import json
import uuid

app = Flask(__name__, static_folder='../frontend/dist')
CORS(app, resources={r"/*": {
    "origins": ["http://localhost:3000", "https://image-tagging-frontend.vercel.app"],
    "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    "allow_headers": ["Content-Type", "Authorization"]
}})
# Image extensions to filter by
IMAGE_EXTENSIONS = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.webp', '.svg', '.tiff']

load_dotenv()

# Configure S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
    region_name=os.environ.get('AWS_REGION', 'us-east-1')
)
S3_BUCKET = os.environ.get('S3_BUCKET_NAME')
print(f"Using S3 Bucket: {S3_BUCKET}")

@app.route("/")
def index():
    return jsonify({
        "message": "Welcome to the API!",
        "status": "running"
    })

@app.route('/api/list-directories', methods=['GET'])
def list_directories():
    """List S3 directories (prefixes)"""
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
        
        return jsonify({"directories": directories})
    except Exception as e:
        print("Error in list-directories:", str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/api/list-subdirectories', methods=['POST'])
def list_subdirectories():
    """List subdirectories (prefixes) within a given S3 prefix"""
    data = request.json
    parent_prefix = data.get('directory', '')
    
    try:
        # Ensure the prefix ends with a slash if it's not empty
        if parent_prefix and not parent_prefix.endswith('/'):
            parent_prefix += '/'
            
        # List objects with the given prefix
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=parent_prefix,
            Delimiter='/'
        )
        
        subdirs = []
        
        # Process common prefixes (folders)
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                # Skip the parent prefix itself
                if prefix['Prefix'] == parent_prefix:
                    continue
                    
                # Extract the folder name from the prefix
                folder_name = prefix['Prefix'][len(parent_prefix):].rstrip('/')
                
                subdirs.append({
                    "path": prefix['Prefix'],
                    "name": folder_name
                })
        
        return jsonify({"subdirectories": subdirs})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/list-images', methods=['POST'])
def list_images():
    """List all images in a specified S3 prefix"""
    data = request.json
    folder_path = data.get('folderPath', '')
    
    try:
        # Ensure the prefix ends with a slash if it's not empty
        if folder_path and not folder_path.endswith('/'):
            folder_path += '/'
            
        # List objects with the given prefix
        response = s3_client.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix=folder_path
        )
        
        image_files = []
        
        # Process objects (files)
        if 'Contents' in response:
            for obj in response['Contents']:
                # Skip "directory" objects (ending with /)
                if obj['Key'].endswith('/'):
                    continue
                    
                # Check if the file is an image
                file_ext = os.path.splitext(obj['Key'])[1].lower()
                if file_ext in IMAGE_EXTENSIONS:
                    # Extract the filename from the key
                    filename = obj['Key'][len(folder_path):] if folder_path else obj['Key']
                    image_files.append(filename)
        
        return jsonify({
            "images": image_files,
            "totalCount": len(image_files),
            "folderPath": folder_path
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/image/<path:folder_path>/<filename>')
def get_image(folder_path, filename):
    """Serve an image file from S3"""
    try:
        # Construct the S3 key
        s3_key = os.path.join(folder_path, filename).replace('\\', '/')
        
        # Get the object from S3
        response = s3_client.get_object(
            Bucket=S3_BUCKET,
            Key=s3_key
        )
        
        # Set the appropriate content type
        content_type = response['ContentType']
        
        # Stream the file content
        return response['Body'].read(), 200, {'Content-Type': content_type}
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/save-categorized', methods=['POST'])
def save_categorized():
    """Save categorized images to new folders in S3 based on categories"""
    data = request.json
    source_folder = data.get('sourceFolder', '')
    categorized_images = data.get('categorizedImages', [])
    
    if not categorized_images:
        return jsonify({"error": "No categorized images provided"}), 400
    
    try:
        # Ensure the source folder ends with a slash if it's not empty
        if source_folder and not source_folder.endswith('/'):
            source_folder += '/'
            
        # Create destination parent folder in S3
        dest_parent = "categorized_images/"
        if source_folder:
            parent_dir = os.path.dirname(source_folder.rstrip('/'))
            if parent_dir:
                dest_parent = parent_dir + "/categorized_images/"
        
        # Process each image
        results = []
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
                s3_client.copy_object(
                    Bucket=S3_BUCKET,
                    CopySource={'Bucket': S3_BUCKET, 'Key': source_key},
                    Key=dest_key
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
        
        return jsonify({
            "results": results,
            "categorizedCount": len([r for r in results if r.get('success', False)]),
            "destinationFolder": dest_parent
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

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

if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8080)
    