from torchvision import datasets, transforms


def load_mnist():
    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.5,), (0.5,))]
    )
    mnist = datasets.MNIST(
        root="./data", train=False, download=True, transform=transform
    )
    return mnist
