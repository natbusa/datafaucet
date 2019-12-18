import os
import tempfile
from urllib.request import Request, urlopen


def download(url, file_format=None, path=None):
    headers = {"User-Agent": "datafaucet downloader/1.0"}

    # infer file format from the if file_format is None:
    if file_format is None:
        filename, file_format = os.path.splitext(url)
        file_format = file_format.replace('.', '')

    req = Request(url, None, headers)
    print(f"Downloading {url}")

    if path:
        f = open(path, 'w')
    else:
        f = tempfile.NamedTemporaryFile(suffix="." + file_format, delete=False)

    try:
        bytes_downloaded = write(urlopen(req), f)
        path = f.name

        if bytes_downloaded > 0:
            print(f"Downloaded {bytes_downloaded} bytes")
    except IOError as e:
        raise e
    finally:
        f.close()

    return path


def write(response, file, chunk_size=8192):
    """
    Load the data from the http request and save it to disk
    :param response: data returned from the server
    :param file:
    :param chunk_size: size chunk size of the data
    :return:
    """

    bytes_written = 0

    while 1:
        chunk = response.read(chunk_size)
        bytes_written += len(chunk)
        if not chunk:
            break

        file.write(chunk)

    return bytes_written
