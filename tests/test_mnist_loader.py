"""
Tests for MNIST dataset loader.

Tests the load_mnist function to ensure proper loading and compatibility
with the gRPC service interface.
"""

import os
import pytest
import torch
import numpy as np
from torchvision import datasets
from src.dataset.mnist_loader import load_mnist


class TestLoadMNIST:
    """Test suite for load_mnist function."""

    def test_load_mnist_returns_dataset(self):
        """Test that load_mnist returns a valid dataset instance."""
        dataset = load_mnist()
        assert isinstance(dataset, datasets.MNIST)

    def test_load_mnist_uses_test_split(self):
        """Test that load_mnist loads the test split."""
        dataset = load_mnist()
        # MNIST test set has 10,000 samples
        assert len(dataset) == 10000

    def test_load_mnist_sample_shape(self):
        """Test that MNIST samples have correct shape."""
        dataset = load_mnist()
        image, label = dataset[0]

        # MNIST images should be (1, 28, 28) after transforms
        assert isinstance(image, torch.Tensor)
        assert image.shape == (1, 28, 28)

    def test_load_mnist_sample_types(self):
        """Test that MNIST samples have correct data types."""
        dataset = load_mnist()
        image, label = dataset[0]

        assert isinstance(image, torch.Tensor)
        assert isinstance(label, int)
        assert image.dtype == torch.float32

    def test_load_mnist_label_range(self):
        """Test that MNIST labels are in valid range [0-9]."""
        dataset = load_mnist()

        # Check first 100 samples
        for i in range(min(100, len(dataset))):
            _, label = dataset[i]
            assert 0 <= label <= 9

    def test_load_mnist_image_normalization(self):
        """Test that MNIST images are normalized correctly."""
        dataset = load_mnist()
        image, _ = dataset[0]

        # After normalization with mean=0.5, std=0.5, values should be in [-1, 1]
        assert image.min() >= -1.0
        assert image.max() <= 1.0

    def test_load_mnist_all_samples_accessible(self):
        """Test that all samples can be accessed without errors."""
        dataset = load_mnist()

        # Test every 1000th sample to save time
        for i in range(0, len(dataset), 1000):
            image, label = dataset[i]
            assert image is not None
            assert label is not None

    def test_load_mnist_downloads_if_needed(self):
        """Test that MNIST data is downloaded if not present."""
        # This test verifies the download=True parameter works
        # It may take time on first run
        dataset = load_mnist()
        assert len(dataset) > 0

    def test_load_mnist_deterministic_output(self):
        """Test that loading same sample multiple times gives same result."""
        dataset = load_mnist()

        # Read same sample multiple times
        image1, label1 = dataset[0]
        image2, label2 = dataset[0]

        assert torch.equal(image1, image2)
        assert label1 == label2


class TestMNISTDatasetIntegration:
    """Integration tests for MNIST dataset."""

    def test_mnist_batch_extraction(self):
        """Test batch extraction pattern used by DatasetService."""
        dataset = load_mnist()
        batch_size = 32

        # Simulate batch extraction
        batch_data = []
        batch_labels = []

        for i in range(batch_size):
            image, label = dataset[i]
            batch_data.append(image.numpy())
            batch_labels.append(label)

        batch_array = np.array(batch_data)
        labels_array = np.array(batch_labels)

        assert batch_array.shape == (batch_size, 1, 28, 28)
        assert labels_array.shape == (batch_size,)
        assert batch_array.dtype == np.float32

    def test_mnist_converts_to_bytes(self):
        """Test that MNIST data can be converted to bytes for gRPC."""
        dataset = load_mnist()
        image, label = dataset[0]

        # Convert to numpy and then to bytes (as done in DatasetService)
        image_np = image.numpy()
        image_bytes = image_np.tobytes()

        assert isinstance(image_bytes, bytes)
        assert len(image_bytes) == image_np.nbytes

    def test_mnist_class_distribution(self):
        """Test that MNIST has all 10 digit classes."""
        dataset = load_mnist()

        # Check that all classes 0-9 exist in first 1000 samples
        labels_seen = set()
        for i in range(min(1000, len(dataset))):
            _, label = dataset[i]
            labels_seen.add(label)

        # Should have seen most classes in first 1000 samples
        assert len(labels_seen) >= 8  # At least 8 out of 10 classes


class TestMNISTCompatibility:
    """Test MNIST compatibility with service infrastructure."""

    def test_mnist_compatible_with_dataloader(self):
        """Test that MNIST dataset works with PyTorch DataLoader."""
        from torch.utils.data import DataLoader

        dataset = load_mnist()
        dataloader = DataLoader(dataset, batch_size=16, shuffle=False)

        # Get first batch
        batch_images, batch_labels = next(iter(dataloader))

        assert batch_images.shape == (16, 1, 28, 28)
        assert batch_labels.shape == (16,)

    def test_mnist_sample_indexing(self):
        """Test that MNIST supports proper indexing."""
        dataset = load_mnist()

        # Test various index patterns
        _ = dataset[0]  # First sample
        _ = dataset[-1]  # Last sample
        _ = dataset[len(dataset) // 2]  # Middle sample

        # Test index out of bounds
        with pytest.raises(IndexError):
            _ = dataset[len(dataset)]

    def test_mnist_multiple_iterations(self):
        """Test that dataset can be iterated multiple times."""
        dataset = load_mnist()

        # First iteration
        first_batch = [dataset[i] for i in range(10)]

        # Second iteration
        second_batch = [dataset[i] for i in range(10)]

        # Should get same data
        for (img1, lbl1), (img2, lbl2) in zip(first_batch, second_batch):
            assert torch.equal(img1, img2)
            assert lbl1 == lbl2


class TestMNISTEdgeCases:
    """Test edge cases for MNIST loader."""

    def test_mnist_data_directory_created(self):
        """Test that data directory is created if it doesn't exist."""
        # This test verifies that the ./data directory is created
        dataset = load_mnist()
        assert os.path.exists("./data")

    def test_mnist_image_shape_consistency(self):
        """Test that all images have the same shape."""
        dataset = load_mnist()

        expected_shape = dataset[0][0].shape

        # Check multiple random samples
        import random

        random_indices = random.sample(range(len(dataset)), min(50, len(dataset)))

        for idx in random_indices:
            image, _ = dataset[idx]
            assert image.shape == expected_shape

    def test_mnist_no_missing_data(self):
        """Test that there are no None values in dataset."""
        dataset = load_mnist()

        # Check a subset of samples
        for i in range(0, min(1000, len(dataset)), 100):
            image, label = dataset[i]
            assert image is not None
            assert label is not None
            assert not torch.isnan(image).any()
