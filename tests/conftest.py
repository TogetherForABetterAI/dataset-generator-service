"""
Pytest configuration and shared fixtures for dataset-generator-service tests.
"""

import os
import sys
import tempfile
import shutil
import h5py
import numpy as np
import pytest

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(autouse=True)
def reset_acdc_global():
    """
    Reset the global ACDC dataset cache before each test.
    This ensures test isolation when testing load_acdc().
    """
    import src.dataset.acdc_loader as acdc_module

    acdc_module._acdc_dataset = None
    yield
    # Reset after test as well
    acdc_module._acdc_dataset = None


@pytest.fixture
def temp_acdc_dir():
    """
    Create a temporary directory with mock ACDC H5 files for testing.

    Yields:
        str: Path to temporary directory containing mock ACDC data
    """
    temp_dir = tempfile.mkdtemp()

    # Create mock ACDC H5 files with realistic structure
    mock_files = [
        ("patient001_frame01_slice_0.h5", 0, (256, 216)),  # background only
        ("patient001_frame01_slice_1.h5", 1, (256, 216)),  # has RV
        ("patient001_frame01_slice_2.h5", 2, (256, 216)),  # has myocardium
        ("patient001_frame01_slice_3.h5", 3, (256, 216)),  # has LV
    ]

    for filename, dominant_class, shape in mock_files:
        filepath = os.path.join(temp_dir, filename)

        with h5py.File(filepath, "w") as f:
            # Create mock image (normalized to [0, 1])
            image = np.random.rand(*shape).astype(np.float32)

            # Create mock label with dominant class
            label = np.zeros(shape, dtype=np.uint8)
            if dominant_class > 0:
                # Add some pixels of the dominant class
                center_y, center_x = shape[0] // 2, shape[1] // 2
                size = 50
                label[
                    center_y - size : center_y + size, center_x - size : center_x + size
                ] = dominant_class

            # Create mock scribble
            scribble = np.random.randint(0, 5, shape, dtype=np.uint16)

            # Save to H5 file
            f.create_dataset("image", data=image)
            f.create_dataset("label", data=label)
            f.create_dataset("scribble", data=scribble)

    yield temp_dir

    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def sample_h5_file():
    """
    Create a single temporary H5 file for testing.

    Yields:
        str: Path to temporary H5 file
    """
    temp_dir = tempfile.mkdtemp()
    filepath = os.path.join(temp_dir, "test_sample.h5")

    with h5py.File(filepath, "w") as f:
        # Create deterministic test data
        image = np.ones((256, 216), dtype=np.float32) * 0.5
        label = np.zeros((256, 216), dtype=np.uint8)
        label[50:150, 50:150] = 2  # Add class 2 in center
        scribble = np.zeros((256, 216), dtype=np.uint16)

        f.create_dataset("image", data=image)
        f.create_dataset("label", data=label)
        f.create_dataset("scribble", data=scribble)

    yield filepath

    # Cleanup
    shutil.rmtree(temp_dir)


@pytest.fixture
def real_acdc_data_path():
    """
    Return path to real ACDC data if available.

    Returns:
        str or None: Path to real data or None if not available
    """
    data_path = "./data/acdc"
    if os.path.exists(data_path) and len(os.listdir(data_path)) > 0:
        return data_path
    return None
