# Dataset Generator Service

## Workflow: Generating gRPC Python Code and Running the Service

### 1. Regenerate gRPC Python Code (when the proto changes)

If you update the `.proto` file (e.g., add a new field or method), you need to regenerate the Python gRPC code. Use the provided script:

```bash
chmod +x src/scripts/gen_proto.sh  # Run this once to make the script executable
sudo src/scripts/gen_proto.sh
```

- This script will delete the old generated code in `src/pb/`, recreate the folder, and run the proto-gen container to generate the new code.
- You only need to do this when the proto file changes.

---

### 2. Build and Run the gRPC Service

After generating (or if no proto changes), build and run the main service as usual:

```bash
docker compose up --build dataset-grpc-service
```

- This will start the gRPC server, ready to serve requests.

---

### Example: Query a Batch with grpcurl

You can test the service using [grpcurl](https://github.com/fullstorydev/grpcurl). For example, to request the last batch of the MNIST dataset (with batch size 2):

```bash
grpcurl -plaintext -emit-defaults -d '{"dataset_name": "mnist", "batch_size": 2, "batch_index": 4999}' localhost:50051 dataset_service.DatasetService/GetBatch
```

This will return a response like:

```json
{
  "data": "...binary...",
  "batch_index": 4999,
  "is_last_batch": true
}
```

# ACDC Dataset Implementation

## Changes Made

### 1. Dependencies Added

**File**: `requirements.txt`

- Added `h5py>=3.10.0` for reading HDF5 files
- Added `pytest>=7.4.0` for testing
- Added `pytest-cov>=4.1.0` for coverage reports

### 2. ACDC Loader Implementation

**File**: `src/dataset/acdc_loader.py`

#### New Custom Dataset Class

Created `ACDCDataset(Dataset)` with the following features:

- Loads cardiac MRI images from H5 files
- Handles segmentation masks with 4 classes (0-3: background, RV, myocardium, LV)
- Supports custom target sizes with proper resizing
- Uses bilinear interpolation for images, nearest neighbor for masks
- Converts segmentation masks to single labels (dominant class)
- Compatible with PyTorch Dataset interface

#### Key Features

- **Input**: H5 files with `image`, `label`, and `scribble` datasets
- **Output**: `(image, label)` tuples compatible with gRPC service
- **Image Format**: Single-channel tensors (1, H, W) normalized to [0, 1]
- **Label Format**: Integer class labels (0-3)
- **Default Size**: 256x256 pixels

#### Error Handling

- Raises `FileNotFoundError` if data directory is missing
- Raises `FileNotFoundError` if no H5 files found
- Validates directory and file existence

### 3. Data Directory Setup

**Structure**:

```
dataset-generator-service/
├── data/
│   └── acdc/
│       ├── patient001_frame01_slice_0.h5
│       ├── patient001_frame01_slice_1.h5
│       ├── ...
│       └── patient001_frame01_slice_6.h5
└── src/
    └── dataset/
        └── acdc/
            ├── notebook.py  (reference implementation)
            └── *.h5  (source files)
```

## Technical Implementation Details

### ACDC Data Loading Process

1. **File Discovery**: Scans directory for `.h5` files
2. **Data Loading**: Opens H5 file, reads `image` and `label` datasets
3. **Tensor Conversion**: Converts numpy arrays to PyTorch tensors
4. **Preprocessing**:
   - Add channel dimension
   - Resize to target size
   - Apply optional transforms
5. **Label Generation**: Extract dominant class from segmentation mask
6. **Return**: `(image, label)` tuple

## Usage

### Running Tests

```bash
# All tests
pytest tests/ -v

# ACDC tests only
pytest tests/test_acdc_loader.py -v

# With coverage
pytest tests/ --cov=src/dataset --cov-report=term-missing
```

## Data Format

### H5 File Structure

```
patient001_frame01_slice_0.h5
├── image     (256, 216) float32 [0.0, 1.0]
├── label     (256, 216) uint8   {0, 1, 2, 3}
└── scribble  (256, 216) uint16  [0, 4]
```

### Class Labels

- **0**: Background
- **1**: Right Ventricle (RV)
- **2**: Myocardium
- **3**: Left Ventricle (LV)
