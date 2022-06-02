#!/usr/bin/env python

import os
import sys
import gzip
import json
import hashlib
import shutil
import tarfile
import urllib3
import pathlib

from dataclasses import dataclass
from argparse import ArgumentParser
from io import BytesIO

import requests
import click

urllib3.disable_warnings()
CHUNK_SIZE = 8192


@dataclass
class URLData:
    auth_url: str
    reg_service: str
    repository: str
    auth_type: str


@dataclass
class ImageData:
    imgparts: str
    registry: str
    repo: str
    img: str
    tag: str
    repository: str

    @property
    def base_url(self):
        return f'https://{self.registry}/v2'

    @property
    def manifest_url(self):
        return f'{self.base_url}/{self.repository}/manifests/{self.tag}'

    @property
    def blobs_url(self):
        return f'{self.base_url}/{self.repository}/blobs'


def get_auth_head(url_data):
    """Get Docker token.

    NOTE: this function is useless for unauthenticated registries like
    Microsoft
    """

    url = (
        f'{url_data.auth_url}'
        f'?service={url_data.reg_service}'
        f'&scope=repository:{url_data.repository}:pull'
    )

    resp = requests.get(url, verify=False)
    access_token = resp.json()['token']

    auth_head = {
        'Authorization': f'Bearer {access_token}',
        'Accept': url_data.auth_type,
    }

    return auth_head


def progress_bar(ublob, nb_traits):
    """Docker style progress bar."""

    # TODO: move to tqdm?

    sys.stdout.write('\r' + ublob[7:19] + ': Downloading [')

    for i in range(0, nb_traits):

        if i == nb_traits - 1:
            sys.stdout.write('>')
        else:
            sys.stdout.write('=')

    for i in range(0, 49 - nb_traits):
        sys.stdout.write(' ')

    sys.stdout.write(']')
    sys.stdout.flush()


def build_options():

    # TODO: move to argparse or click?
    if len(sys.argv) != 2 :
        print(
            'Usage:\n'
            '\tdocker_pull.py \n'
        )
        sys.exit(1)


def parse_image(image):

    # Look for the Docker image to download
    repo = 'library'
    tag = 'latest'

    imgparts = image.split('/')
    try:
        img, tag = imgparts[-1].split('@')
    except ValueError:
        try:
            img, tag = imgparts[-1].split(':')
        except ValueError:
            img = imgparts[-1]

    # Docker client doesn't seem to consider the first element as a potential
    # registry unless there is a '.' or ':'
    if len(imgparts) > 1 and ('.' in imgparts[0] or ':' in imgparts[0]):
        registry = imgparts[0]
        repo = '/'.join(imgparts[1:-1])

    else:
        registry = 'registry-1.docker.io'
        if len(imgparts[:-1]) != 0:
            repo = '/'.join(imgparts[:-1])
        else:
            repo = 'library'

    repository = f'{repo}/{img}'

    return ImageData(imgparts, registry, repo, img, tag, repository)


def manifest_error(resp, url_data, image_data):

    print(
        f'[-] Cannot fetch manifest for {url_data.repository} '
        f'[HTTP {resp.status_code}]'
    )
    print(resp.content)

    auth_type = 'application/vnd.docker.distribution.manifest.list.v2+json'
    url_data.auth_type = auth_type
    auth_head = get_auth_head(url_data)

    url = f'https://{registry}/v2/{image_data.repository}/manifests/{tag}'
    resp = requests.get(url, headers=auth_head, verify=False)

    if (resp.status_code == 200):
        print(
            '[+] Manifests found for this tag (use the @digest format to '
            'pull the corresponding image):'
        )

        manifests = resp.json()['manifests']
        for manifest in manifests:
            for key, value in manifest["platform"].items():
                sys.stdout.write(f'{key}: {value}, ')

            print(f'digest: {manifest["digest"]}')

    sys.exit(1)


def get_base_json():

    container_config = {
        "Hostname": "",
        "Domainname": "",
        "User": "",
        "AttachStdin": False,
        "AttachStdout": False,
        "AttachStderr": False,
        "Tty": False,
        "OpenStdin": False,
        "StdinOnce": False,
        "Env": None,
        "Cmd": None,
        "Image": "",
        "Volumes": None,
        "WorkingDir": "",
        "Entrypoint": None,
        "OnBuild": None,
        "Labels": None,
    }

    empty_dict = {
        "created": "1970-01-01T00:00:00Z",
        "container_config":  container_config,
    }

    return json.dumps(empty_dict)


def get_url_data(image_data):

    # Get Docker authentication endpoint when it is required
    auth_url='https://auth.docker.io/token'
    reg_service='registry.docker.io'

    resp = requests.get(image_data.base_url, verify=False)

    if resp.status_code == 401:
        auth_url = resp.headers['WWW-Authenticate'].split('"')[1]
        try:
            reg_service = resp.headers['WWW-Authenticate'].split('"')[3]
        except IndexError:
            reg_service = ""

    # Fetch manifest v2 and get image layer digests
    auth_type = 'application/vnd.docker.distribution.manifest.v2+json'

    url_data = URLData(
        auth_url=auth_url,
        reg_service=reg_service,
        repository=image_data.repository,
        auth_type=auth_type,
    )

    return url_data


def save_chunks(layerdir, bresp, ublob, nb_traits):

    unit = int(bresp.headers['Content-Length']) / 50
    acc = 0

    layer_file = layerdir / 'layer_gzip.tar'
    with open(layer_file, "wb") as file:

        for chunk in bresp.iter_content(chunk_size=CHUNK_SIZE):

            if not chunk:
                continue

            file.write(chunk)
            acc = acc + CHUNK_SIZE
            if acc > unit:
                nb_traits = nb_traits + 1
                progress_bar(ublob, nb_traits)
                acc = 0


def layer_error(layer, auth_head):

    bresp = requests.get(
        layer['urls'][0],
        headers=auth_head,
        stream=True,
        verify=False
    )

    if bresp.status_code == 200:
        return bresp

    err_msg = (
        f'\rERROR: Cannot download layer {ublob[7:19]} '
        f'[HTTP {bresp.status_code}]'
    )
    print(err_msg)
    print(bresp.content)
    sys.exit(1)


def get_fake_layerid():

    # FIXME: Creating fake layer ID. Don't know how Docker generates it
    fake_layerid = "{parentid}\n"
    fake_layerid += "{ublob}\n"
    fake_layerid = fake_layerid.encode('utf-8')
    fake_layerid = hashlib.sha256(fake_layerid)
    fake_layerid = fake_layerid.hexdigest()
    return fake_layerid


def func_layers(layers, url_data, image_data, imgdir, content, confresp):

    # TODO: better function name...

    # Build layer folders
    parentid = ''
    for layer in layers:

        ublob = layer['digest']
        fake_layerid = get_fake_layerid()

        base_path = pathlib.Path(__file__).absolute().parent
        layerdir = base_path / imgdir / fake_layerid
        layerdir.mkdir()

        # Creating VERSION file
        layer_version = layerdir / 'VERSION'
        with open(layer_version, 'w') as file:
            file.write('1.0')

        # Creating layer.tar file
        sys.stdout.write(ublob[7:19] + ': Downloading...')
        sys.stdout.flush()

        # refreshing token to avoid its expiration
        auth_type = 'application/vnd.docker.distribution.manifest.v2+json'
        auth_head = get_auth_head(url_data)

        url = f"{image_data.blobs_url}/{ublob}"
        bresp = requests.get(url, headers=auth_head, stream=True, verify=False)

        # When the layer is located at a custom URL
        if bresp.status_code != 200:
            bresp = layer_error(layer, auth_head)

        # Stream download and follow the progress
        bresp.raise_for_status()
        nb_traits = 0
        progress_bar(ublob, nb_traits)

        save_chunks(layerdir, bresp, ublob, nb_traits)

        # Ugly but works everywhere
        msg = f"\r{ublob[7:19]}: Extracting...{' '*50}"
        sys.stdout.write(msg)
        sys.stdout.flush()

        # Decompress gzip response
        layer_tar_file = layerdir / 'layer.tar'
        layer_gzip_file = layerdir / 'layer_gzip.tar'

        with open(layer_tar_file, "wb") as file:
            unzLayer = gzip.open(layer_gzip_file, 'rb')
            shutil.copyfileobj(unzLayer, file)
            unzLayer.close()

        layer_gzip_file.unlink()
        msg = (
            f"\r{ublob[7:19],}: Pull complete "
            f"[{bresp.headers['Content-Length']}]"
        )
        print(msg)

        content[0]['Layers'].append(fake_layerid + '/layer.tar')

        # last layer = config manifest - history - rootfs
        if layers[-1]['digest'] == layer['digest']:

            # FIXME: json.loads() automatically converts to unicode, thus
            # decoding values whereas Docker doesn't

            json_obj = json.loads(confresp.content)
            del json_obj['history']
            try:
                del json_obj['rootfs']
            except:  # Because Microsoft loves case insensitiveness
                del json_obj['rootfS']

        else:  # other layers json are empty
            json_obj = json.loads(empty_json)

        json_obj['id'] = fake_layerid
        if parentid:
            json_obj['parent'] = parentid

        parentid = json_obj['id']

        # Creating json file
        json_file = layerdir / 'json'
        with open(json_file, 'w') as file:
            json.dump(json_obj, file)

    return fake_layerid

def save_image_tar(repo, img, imgdir):

    # Create image tar and clean tmp folder
    docker_tar = repo.replace('/', '_') + '_' + img + '.tar'
    sys.stdout.write("Creating archive...")
    sys.stdout.flush()

    tar = tarfile.open(docker_tar, "w")
    tar.add(imgdir, arcname=os.path.sep)
    tar.close()


def get_content_json(image_data, fake_layerid):

    imgparts = image_data.imgparts
    img = image_data.img
    tag = image_data.tag

    if len(imgparts[:-1]) != 0:
        content = {
            '/'.join(imgparts[:-1]) + '/' + img : {
                tag : fake_layerid
            }
        }

    else:  # when pulling only an img (without repo and registry)
        content = {
            img: {
                tag: fake_layerid
            }
        }

    return content


CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.command(no_args_is_help=True, context_settings=CONTEXT_SETTINGS)
@click.argument("image")
def main(image):
    """Download a docker image.

    \b
    Usage:
        [registry/][repository/]image[:tag|@digest]
    """

    image_data = parse_image(image)
    url_data = get_url_data(image_data)
    auth_head = get_auth_head(url_data)

    resp = requests.get(
        image_data.manifest_url,
        headers=auth_head,
        verify=False
    )

    if resp.status_code != 200:
        manifest_error(resp, url_data, image_data)

    layers = resp.json()['layers']

    # Create tmp folder that will hold the image
    img_tag = image_data.tag.replace(':', '@')
    basedir = pathlib.Path(__file__).absolute().parent
    imgdir = basedir / f"tmp_{image_data.img}_{img_tag}"
    imgdir.mkdir()
    print(f'Creating image structure in: {imgdir}')

    config = resp.json()
    config = config['config']['digest']
    config_base = config[7:]

    url = f'{image_data.blobs_url}/{config}'
    confresp = requests.get(url, headers=auth_head, verify=False)

    filename = f'{imgdir}/{config_base}.json'
    with open(filename, 'wb') as file:
        file.write(confresp.content)

    content = {
        'Config': f'{config_base}.json',
        'RepoTags': [],
        'Layers': [],
    }
    content = [content]

    if len(image_data.imgparts[:-1]) != 0:
        path = '/'.join(imgparts[:-1]) + '/' + img + ':' + tag
    else:
        path = image_data.img + ':' + image_data.tag

    content[0]['RepoTags'].append(path)
    empty_json = get_base_json()
    fake_layerid = func_layers(
        layers,
        url_data,
        image_data,
        imgdir,
        content,
        confresp
    )

    manifest_file = imgdir / 'manifest.json'
    with open(manifest_file, 'w') as file:
        file.write(json.dumps(content))

    content = get_content_json(image_data, fake_layerid)

    repositories_file = imgdir / 'repositories'
    with open(repositories_file, 'w') as file:
        json.dump(content, file)

    save_image_tar(image_data.repo, image_data.img, imgdir)

    # clean up
    shutil.rmtree(imgdir)

    print('\rDocker image pulled: {docker_tar}')


if __name__ == "__main__":
    main()
