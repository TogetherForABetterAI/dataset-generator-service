import os
import h5py
import numpy as np
import torch
from torch.utils.data import Dataset
from torchvision import transforms


class ACDCDataset(Dataset):
    """
    Custom PyTorch Dataset for ACDC (Automated Cardiac Diagnosis Challenge) dataset.

    Loads cardiac MRI images and segmentation labels from H5 files.
    Each H5 file contains:
    - 'image': 2D cardiac MRI slice (already normalized to [0, 1])
    - 'label': segmentation mask with classes 0-3 (background, RV, myocardium, LV)
    - 'scribble': scribble annotations (optional, not used for training)
    """

    def __init__(self, root_dir, target_size=(256, 256), transform=None):
        """
        Args:
            root_dir (str): Directory containing H5 files
            target_size (tuple): Target size for resizing images (height, width)
            transform (callable, optional): Optional transform to be applied on images
        """
        self.root_dir = root_dir
        self.target_size = target_size
        self.transform = transform

        # Get all H5 files in directory
        self.h5_files = sorted([f for f in os.listdir(root_dir) if f.endswith(".h5")])

        if len(self.h5_files) == 0:
            raise FileNotFoundError(
                f"No H5 files found in {root_dir}. "
                "Please ensure ACDC dataset files are present."
            )

        print(f"Loaded ACDC dataset with {len(self.h5_files)} slices from {root_dir}")

    def __len__(self):
        return len(self.h5_files)

    def __getitem__(self, idx):
        """
        Returns:
            image (torch.Tensor): Preprocessed image of shape (C, H, W)
            label (int): Dominant class in the segmentation mask
        """
        h5_path = os.path.join(self.root_dir, self.h5_files[idx])

        with h5py.File(h5_path, "r") as f:
            # Load image (already normalized to [0, 1])
            image = f["image"][:]  # Shape: (H, W)
            seg_mask = f["label"][:]  # Shape: (H, W)

        # Convert to torch tensors
        image = torch.from_numpy(image).float()
        seg_mask = torch.from_numpy(seg_mask).long()

        # Add channel dimension: (H, W) -> (1, H, W)
        image = image.unsqueeze(0)

        # Resize if needed
        if image.shape[1:] != self.target_size:
            # Use bilinear interpolation for images
            image = torch.nn.functional.interpolate(
                image.unsqueeze(0),
                size=self.target_size,
                mode="bilinear",
                align_corners=False,
            ).squeeze(0)

            # Use nearest neighbor for segmentation masks
            seg_mask = (
                torch.nn.functional.interpolate(
                    seg_mask.unsqueeze(0).unsqueeze(0).float(),
                    size=self.target_size,
                    mode="nearest",
                )
                .squeeze(0)
                .squeeze(0)
                .long()
            )

        # Apply additional transforms if provided
        if self.transform:
            image = self.transform(image)

        # Convert segmentation mask to a single label (dominant class)
        # This simplifies the interface for classification-style evaluation
        unique_classes, counts = torch.unique(seg_mask, return_counts=True)
        # Exclude background (0) if other classes exist
        if len(unique_classes) > 1:
            non_background_mask = unique_classes != 0
            if non_background_mask.any():
                filtered_classes = unique_classes[non_background_mask]
                filtered_counts = counts[non_background_mask]
                label = filtered_classes[torch.argmax(filtered_counts)].item()
            else:
                label = 0
        else:
            label = unique_classes[0].item()

        return image, label


def load_acdc():
    """
    Load ACDC (Automated Cardiac Diagnosis Challenge) dataset.

    Returns:
        ACDCDataset: PyTorch Dataset containing cardiac MRI slices
    """
    # Path to ACDC data relative to the service root
    acdc_data_path = "./data/acdc"

    # Check if data exists
    if not os.path.exists(acdc_data_path):
        raise FileNotFoundError(
            f"ACDC dataset not found at {acdc_data_path}. "
            "Please ensure the H5 files are placed in the correct directory."
        )

    # Optional: Define transforms for additional preprocessing
    # Images are already normalized to [0, 1] in the H5 files
    transform = None  # Can add transforms here if needed

    # Create and return the dataset
    acdc_dataset = ACDCDataset(
        root_dir=acdc_data_path,
        target_size=(256, 256),  # Standard size for ACDC
        transform=transform,
    )

    return acdc_dataset
