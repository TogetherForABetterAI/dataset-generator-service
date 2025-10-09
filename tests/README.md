# Dataset Generator Service Tests

This directory contains test suites for the dataset loaders used in the dataset-generator-service.

## Test Structure

```
tests/
├── __init__.py                 # Package initialization
├── conftest.py                 # Shared pytest fixtures
├── test_acdc_loader.py        # Tests for ACDC dataset loader
└── test_mnist_loader.py       # Tests for MNIST dataset loader
```

## Running Tests

### Prerequisites

Install test dependencies:

```bash
pip install -r requirements.txt
```

### Run All Tests

```bash
# Run all tests with coverage
pytest tests/ -v --cov=src/dataset --cov-report=term-missing

# Run all tests without coverage (faster)
pytest tests/ -v --no-cov
```

### Run Specific Test Files

```bash
# Test ACDC loader only
pytest tests/test_acdc_loader.py -v

# Test MNIST loader only
pytest tests/test_mnist_loader.py -v
```

### Run Specific Test Classes or Methods

```bash
# Run specific test class
pytest tests/test_acdc_loader.py::TestACDCDataset -v

# Run specific test method
pytest tests/test_acdc_loader.py::TestACDCDataset::test_getitem_returns_correct_shape -v
```

## Test Coverage

Current test coverage for dataset loaders:

- **ACDC Loader**: 98% coverage (47/47 statements)
- **MNIST Loader**: 100% coverage (5/5 statements)

## Test Categories

### ACDC Loader Tests (`test_acdc_loader.py`)

**TestACDCDataset** - Core functionality tests:

- Initialization with valid/invalid directories
- Custom target sizes
- Data type validation
- Shape verification
- Value range checks
- Label validation
- Transform application

**TestLoadACDC** - Integration tests:

- Dataset loading
- Error handling
- Default parameters

**TestACDCDatasetIntegration** - Real data tests:

- Loading actual ACDC data (if available)
- Batch extraction patterns
- gRPC service compatibility

**TestACDCDatasetEdgeCases** - Edge case handling:

- Single file datasets
- Deterministic output
- Non-square resize operations

### MNIST Loader Tests (`test_mnist_loader.py`)

**TestLoadMNIST** - Basic functionality:

- Dataset instantiation
- Test split verification
- Sample shape and types
- Normalization validation
- Data accessibility

**TestMNISTDatasetIntegration** - Integration tests:

- Batch extraction
- Byte conversion (for gRPC)
- Class distribution

**TestMNISTCompatibility** - Infrastructure tests:

- DataLoader compatibility
- Indexing operations
- Multiple iterations

**TestMNISTEdgeCases** - Edge cases:

- Directory creation
- Shape consistency
- Missing data detection

## Fixtures

### Shared Fixtures (defined in `conftest.py`)

- **`temp_acdc_dir`**: Creates temporary directory with mock ACDC H5 files
- **`sample_h5_file`**: Creates a single temporary H5 file for testing
- **`real_acdc_data_path`**: Returns path to real ACDC data if available

## Test Data

### Mock Data

Tests use dynamically generated mock data with realistic structure:

- **ACDC**: Mock H5 files with `image`, `label`, and `scribble` datasets
- **MNIST**: Uses PyTorch's built-in MNIST dataset with automatic download

### Real Data

Integration tests can run with real ACDC data if available at `./data/acdc/`.
These tests are automatically skipped if real data is not present.

## Writing New Tests

When adding new dataset loaders, follow this pattern:

1. **Create test file**: `test_<loader_name>_loader.py`
2. **Organize tests by category**:
   - Basic functionality
   - Integration tests
   - Compatibility tests
   - Edge cases
3. **Use fixtures**: Define shared fixtures in `conftest.py`
4. **Add markers**: Use `@pytest.mark.skipif` for optional tests
5. **Test gRPC compatibility**: Ensure batch extraction works as expected

## CI/CD Integration

Tests are designed to run in CI/CD pipelines:

- No external dependencies required (datasets auto-download)
- Mock data generated on-the-fly
- Fast execution (< 10 seconds for all tests)
- Clear failure messages

## Troubleshooting

### Import Errors

If you see import errors, ensure you're running tests from the service root:

```bash
cd dataset-generator-service
pytest tests/
```

### Missing Real Data

Tests requiring real ACDC data will be skipped automatically:

```
tests/test_acdc_loader.py::TestACDCDatasetIntegration::test_load_real_acdc_data SKIPPED
```

To enable these tests, place ACDC H5 files in `./data/acdc/`.

### H5py Issues

If H5py fails to load, ensure it's installed:

```bash
pip install h5py>=3.10.0
```

## Additional Commands

```bash
# Run with verbose output
pytest tests/ -vv

# Show print statements
pytest tests/ -s

# Run in parallel (requires pytest-xdist)
pytest tests/ -n auto

# Generate HTML coverage report
pytest tests/ --cov=src/dataset --cov-report=html
# View report at htmlcov/index.html
```
