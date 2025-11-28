import os
import threading
from torchvision import datasets, transforms

# Global lock to prevent concurrent downloads
_mnist_lock = threading.Lock()
_mnist_dataset = None


def load_mnist():
    """
    Load MNIST dataset with thread-safe download.
    Only one thread/process will download, others will wait.
    """
    global _mnist_dataset

    # Fast path: if already loaded, return immediately
    if _mnist_dataset is not None:
        return _mnist_dataset

    # Acquire lock to ensure only one thread downloads
    with _mnist_lock:
        # Double-check pattern: dataset might have been loaded while waiting for lock
        if _mnist_dataset is not None:
            return _mnist_dataset

        transform = transforms.Compose(
            [transforms.ToTensor(), transforms.Normalize((0.5,), (0.5,))]
        )

        # Download and load MNIST
        mnist = datasets.MNIST(
            root="./data", train=False, download=True, transform=transform
        )

        _mnist_dataset = mnist
        return mnist
