"""
Tests for ACDC dataset loader.

Tests the ACDCDataset class and load_acdc function to ensure proper
loading, preprocessing, and compatibility with the gRPC service interface.
"""

import os
import pytest
import torch
import numpy as np
from src.dataset.acdc_loader import ACDCDataset, load_acdc


class TestACDCDataset:
    """Test suite for ACDCDataset class."""

    def test_init_with_valid_directory(self, temp_acdc_dir):
        """Test initialization with a valid directory containing H5 files."""
        dataset = ACDCDataset(root_dir=temp_acdc_dir)
        assert len(dataset) == 4  # 4 mock files created in fixture
        assert dataset.root_dir == temp_acdc_dir
        assert dataset.target_size == (256, 256)
        assert len(dataset.h5_files) == 4

    def test_init_with_custom_target_size(self, temp_acdc_dir):
        """Test initialization with custom target size."""
        custom_size = (128, 128)
        dataset = ACDCDataset(root_dir=temp_acdc_dir, target_size=custom_size)
        assert dataset.target_size == custom_size

    def test_init_with_empty_directory(self, tmp_path):
        """Test initialization with empty directory raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="No H5 files found"):
            ACDCDataset(root_dir=str(tmp_path))

    def test_init_with_nonexistent_directory(self):
        """Test initialization with non-existent directory raises error."""
        with pytest.raises((FileNotFoundError, OSError)):
            ACDCDataset(root_dir="/nonexistent/path")

    def test_len(self, temp_acdc_dir):
        """Test __len__ returns correct number of samples."""
        dataset = ACDCDataset(root_dir=temp_acdc_dir)
        assert len(dataset) == 4

    def test_getitem_returns_correct_types(self, temp_acdc_dir):
        """Test __getitem__ returns correct data types."""
        dataset = ACDCDataset(root_dir=temp_acdc_dir)
        image, label = dataset[0]

        assert isinstance(image, torch.Tensor)
        assert isinstance(label, int)
        assert image.dtype == torch.float32

    def test_getitem_returns_correct_shape(self, temp_acdc_dir):
        """Test __getitem__ returns correctly shaped images."""
        dataset = ACDCDataset(root_dir=temp_acdc_dir)
        image, label = dataset[0]

        # Should be (1, H, W) - single channel
        assert image.shape == (1, 256, 256)

    def test_getitem_with_custom_size(self, temp_acdc_dir):
        """Test __getitem__ respects custom target size."""
        custom_size = (128, 128)
        dataset = ACDCDataset(root_dir=temp_acdc_dir, target_size=custom_size)
        image, label = dataset[0]

        assert image.shape == (1, 128, 128)

    def test_getitem_image_value_range(self, temp_acdc_dir):
        """Test that image values are in valid range [0, 1]."""
        dataset = ACDCDataset(root_dir=temp_acdc_dir)
        image, label = dataset[0]

        assert image.min() >= 0.0
        assert image.max() <= 1.0

    def test_getitem_label_values(self, temp_acdc_dir):
        """Test that labels are valid ACDC classes (0-3)."""
        dataset = ACDCDataset(root_dir=temp_acdc_dir)

        for i in range(len(dataset)):
            image, label = dataset[i]
            assert 0 <= label <= 3
            assert isinstance(label, int)

    def test_getitem_all_samples_accessible(self, temp_acdc_dir):
        """Test that all samples can be accessed without errors."""
        dataset = ACDCDataset(root_dir=temp_acdc_dir)

        for i in range(len(dataset)):
            image, label = dataset[i]
            assert image is not None
            assert label is not None

    def test_getitem_index_out_of_bounds(self, temp_acdc_dir):
        """Test that accessing invalid index raises IndexError."""
        dataset = ACDCDataset(root_dir=temp_acdc_dir)

        with pytest.raises((IndexError, KeyError)):
            _ = dataset[len(dataset)]

    def test_h5_files_sorted(self, temp_acdc_dir):
        """Test that H5 files are sorted for deterministic ordering."""
        dataset = ACDCDataset(root_dir=temp_acdc_dir)
        assert dataset.h5_files == sorted(dataset.h5_files)

    def test_transform_applied_if_provided(self, temp_acdc_dir):
        """Test that custom transform is applied to images."""

        # Define a simple transform that multiplies by 2
        def double_transform(img):
            return img * 2

        dataset = ACDCDataset(root_dir=temp_acdc_dir, transform=double_transform)
        image, _ = dataset[0]

        # Values should be doubled (but may be clipped)
        assert image.max() > 1.0 or image.mean() > 0.5


class TestLoadACDC:
    """Test suite for load_acdc function."""

    def test_load_acdc_returns_dataset(self, monkeypatch, temp_acdc_dir):
        """Test that load_acdc returns an ACDCDataset instance."""
        # Mock the data path to use our temp directory
        monkeypatch.setattr("src.dataset.acdc_loader.os.path.exists", lambda x: True)

        # Create a mock that returns our dataset
        original_acdc_dataset = ACDCDataset

        def mock_acdc_dataset(root_dir, **kwargs):
            return original_acdc_dataset(root_dir=temp_acdc_dir, **kwargs)

        monkeypatch.setattr("src.dataset.acdc_loader.ACDCDataset", mock_acdc_dataset)

        dataset = load_acdc()
        assert isinstance(dataset, ACDCDataset)

    def test_load_acdc_raises_error_if_path_not_exists(self, monkeypatch):
        """Test that load_acdc raises FileNotFoundError if data path doesn't exist."""
        # Mock os.path.exists to return False
        monkeypatch.setattr("src.dataset.acdc_loader.os.path.exists", lambda x: False)

        with pytest.raises(FileNotFoundError, match="ACDC dataset not found"):
            load_acdc()

    def test_load_acdc_default_parameters(self, monkeypatch, temp_acdc_dir):
        """Test that load_acdc uses correct default parameters."""
        monkeypatch.setattr("src.dataset.acdc_loader.os.path.exists", lambda x: True)

        # Track what parameters were used
        params_used = {}

        original_acdc_dataset = ACDCDataset

        def mock_acdc_dataset(root_dir, **kwargs):
            params_used.update(kwargs)
            return original_acdc_dataset(root_dir=temp_acdc_dir, **kwargs)

        monkeypatch.setattr("src.dataset.acdc_loader.ACDCDataset", mock_acdc_dataset)

        dataset = load_acdc()

        assert params_used.get("target_size") == (256, 256)
        assert params_used.get("transform") is None


class TestACDCDatasetIntegration:
    """Integration tests with real data if available."""

    @pytest.mark.skipif(
        not os.path.exists("./data/acdc"), reason="Real ACDC data not available"
    )
    def test_load_real_acdc_data(self):
        """Test loading real ACDC data if available."""
        dataset = load_acdc()
        assert len(dataset) > 0

        # Test loading first sample
        image, label = dataset[0]
        assert image.shape[0] == 1  # Single channel
        assert image.shape[1:] == (256, 256)
        assert 0 <= label <= 3

    @pytest.mark.skipif(
        not os.path.exists("./data/acdc"), reason="Real ACDC data not available"
    )
    def test_real_data_batch_extraction(self):
        """Test batch extraction pattern used by DatasetService."""
        dataset = load_acdc()
        batch_size = 2

        # Simulate batch extraction
        batch_data = []
        batch_labels = []

        for i in range(min(batch_size, len(dataset))):
            image, label = dataset[i]
            batch_data.append(image.numpy())
            batch_labels.append(label)

        batch_array = np.array(batch_data)
        labels_array = np.array(batch_labels)

        assert batch_array.shape[0] == min(batch_size, len(dataset))
        assert labels_array.shape[0] == min(batch_size, len(dataset))
        assert batch_array.dtype == np.float32


class TestACDCDatasetEdgeCases:
    """Test edge cases and error handling."""

    def test_dataset_with_single_file(self, tmp_path, sample_h5_file):
        """Test dataset with only one H5 file."""
        # Copy single file to temp directory
        import shutil

        dest_file = tmp_path / "single_file.h5"
        shutil.copy(sample_h5_file, dest_file)

        dataset = ACDCDataset(root_dir=str(tmp_path))
        assert len(dataset) == 1

        image, label = dataset[0]
        assert image is not None
        assert label is not None

    def test_dataset_deterministic_output(self, sample_h5_file):
        """Test that reading the same sample multiple times gives same result."""
        import shutil
        import tempfile

        temp_dir = tempfile.mkdtemp()
        dest_file = os.path.join(temp_dir, "test_file.h5")
        shutil.copy(sample_h5_file, dest_file)

        dataset = ACDCDataset(root_dir=temp_dir)

        # Read same sample multiple times
        image1, label1 = dataset[0]
        image2, label2 = dataset[0]

        assert torch.equal(image1, image2)
        assert label1 == label2

        # Cleanup
        shutil.rmtree(temp_dir)

    def test_resize_preserves_aspect_ratio_handling(self, temp_acdc_dir):
        """Test that resize handles non-square target sizes."""
        dataset = ACDCDataset(root_dir=temp_acdc_dir, target_size=(128, 256))
        image, _ = dataset[0]

        assert image.shape == (1, 128, 256)
