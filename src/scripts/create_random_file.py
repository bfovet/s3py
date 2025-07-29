import os
import random
import tempfile


def generate_random_file():
    size_mb = 1024
    extension = "bin"

    # Calculate the file size in bytes
    file_size = size_mb * 1024 * 1024

    # Generate a chunk of random bytes to write to the file
    chunk_size = 1024 * 1024
    chunk = bytearray(random.getrandbits(8) for _ in range(chunk_size))

    # Create a temporary directory and file path
    temp_dir = tempfile.mkdtemp()
    file_path = os.path.join(temp_dir, "random_file.bin")

    # Write the random data to the file until it reaches the desired size
    with open(file_path, "wb") as f:
        while os.path.getsize(file_path) < file_size:
            f.write(chunk)

    # Move the file to the desired location with the specified extension
    final_path = f"./file.{extension}"
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    os.replace(file_path, final_path)


if __name__ == "__main__":
    generate_random_file()
