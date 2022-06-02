#!/usr/bin/env python

import os
import sys
import gzip
from io import BytesIO
import json
import hashlib
import shutil
import requests
import tarfile
import urllib3
urllib3.disable_warnings()


def get_auth_head(auth_url, reg_service, repository, auth_type):
    """Get Docker token.

    NOTE: this function is useless for unauthenticated registries like
    Microsoft
    """

    url = f'{auth_url}?service={reg_service}&scope=repository:{repository}:pull'
    resp = requests.get(url, verify=False)
    access_token = resp.json()['token']

    auth_head = {
        'Authorization': f'Bearer {access_token}',
        'Accept': auth_type,
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


def main():

    # TODO: move to argparse or click?
    if len(sys.argv) != 2 :
        print(
            'Usage:\n'
            '\tdocker_pull.py [registry/][repository/]image[:tag|@digest]\n'
        )
        sys.exit(1)

    # Look for the Docker image to download
    repo = 'library'
    tag = 'latest'
    imgparts = sys.argv[1].split('/')

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

    # Get Docker authentication endpoint when it is required
    auth_url='https://auth.docker.io/token'
    reg_service='registry.docker.io'

    url = f'https://{registry}/v2/'
    resp = requests.get(url, verify=False)
    if resp.status_code == 401:
        auth_url = resp.headers['WWW-Authenticate'].split('"')[1]
        try:
            reg_service = resp.headers['WWW-Authenticate'].split('"')[3]
        except IndexError:
            reg_service = ""

    # Fetch manifest v2 and get image layer digests
    auth_type = 'application/vnd.docker.distribution.manifest.v2+json'
    auth_head = get_auth_head(auth_url, reg_service, repository, auth_type)

    url = f'https://{registry}/v2/{repository}/manifests/{tag}'
    resp = requests.get(url, headers=auth_head, verify=False)


    if (resp.status_code != 200):

        print(
            f'[-] Cannot fetch manifest for {repository} '
            f'[HTTP {resp.status_code}]'
        )
        print(resp.content)

        auth_type = 'application/vnd.docker.distribution.manifest.list.v2+json'
        auth_head = get_auth_head(auth_url, reg_service, repository, auth_type)

        url = f'https://{registry}/v2/{repository}/manifests/{tag}'
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

    layers = resp.json()['layers']

    # Create tmp folder that will hold the image

    # TODO: Fix this string
    imgdir = f"tmp_{img}_{tag.replace(':', '@')}"
    os.mkdir(imgdir)
    print('Creating image structure in: {imgdir}')

    config = resp.json()['config']['digest']
    url = f'https://{registry}/v2/{repository}/blobs/{config}'
    confresp = requests.get(url, headers=auth_head, verify=False)

    filename = f'{imgdir}/{config[7:]}.json'
    with open(filename, 'wb') as file:
        file.write(confresp.content)

    content = [{
        'Config': config[7:] + '.json',
        'RepoTags': [],
        'Layers': [],
    }]

    if len(imgparts[:-1]) != 0:
        content[0]['RepoTags'].append('/'.join(imgparts[:-1]) + '/' + img + ':' + tag)
    else:
        content[0]['RepoTags'].append(img + ':' + tag)

    empty_dict = {
        "created": "1970-01-01T00:00:00Z",
        "container_config": {
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
            "Labels": None
        }
    }

    empty_json = json.dumps(empty_dict)

    # Build layer folders
    parentid=''
    for layer in layers:

        ublob = layer['digest']
        # FIXME: Creating fake layer ID. Don't know how Docker generates it
        fake_layerid = hashlib.sha256((parentid+'\n'+ublob+'\n').encode('utf-8'))
        fake_layerid = fake_layerid.hexdigest()

        layerdir = imgdir + '/' + fake_layerid
        os.mkdir(layerdir)

        # Creating VERSION file
        with open(layerdir + '/VERSION', 'w') as file:
            file.write('1.0')

        # Creating layer.tar file
        sys.stdout.write(ublob[7:19] + ': Downloading...')
        sys.stdout.flush()

        # refreshing token to avoid its expiration
        auth_type = 'application/vnd.docker.distribution.manifest.v2+json'
        auth_head = get_auth_head(auth_url, reg_service, repository, auth_type)

        url = 'https://{registry}/v2/{repository}/blobs/{ublob}'
        bresp = requests.get(url, headers=auth_head, stream=True, verify=False)

        # When the layer is located at a custom URL
        if bresp.status_code != 200:

            bresp = requests.get(
                layer['urls'][0],
                headers=auth_head,
                stream=True,
                verify=False
            )

            if (bresp.status_code != 200):
                err_msg = (
                    '\rERROR: Cannot download layer {ublob[7:19]} '
                    '[HTTP {bresp.status_code}]'
                )
                print(err_msg)
                print(bresp.content)
                sys.exit(1)

        # Stream download and follow the progress
        bresp.raise_for_status()
        unit = int(bresp.headers['Content-Length']) / 50
        acc = 0
        nb_traits = 0
        progress_bar(ublob, nb_traits)

        CHUNK_SIZE = 8192
        with open(layerdir + '/layer_gzip.tar', "wb") as file:
            for chunk in bresp.iter_content(chunk_size=CHUNK_SIZE):

                if not chunk:
                    continue

                file.write(chunk)
                acc = acc + CHUNK_SIZE
                if acc > unit:
                    nb_traits = nb_traits + 1
                    progress_bar(ublob, nb_traits)
                    acc = 0

        # Ugly but works everywhere
        msg = f"\r{ublob[7:19]}: Extracting...{' '*50}"
        sys.stdout.write(msg)
        sys.stdout.flush()

        # Decompress gzip response
        with open(layerdir + '/layer.tar', "wb") as file:
            filename = layerdir + '/layer_gzip.tar'
            unzLayer = gzip.open(filename, 'rb')
            shutil.copyfileobj(unzLayer, file)
            unzLayer.close()

        os.remove(layerdir + '/layer_gzip.tar')
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
        with open(layerdir + '/json', 'w') as file:
            file.write(json.dumps(json_obj))

    with open(imgdir + '/manifest.json', 'w') as file:
        file.write(json.dumps(content))

    if len(imgparts[:-1]) != 0:
        content = {
            '/'.join(imgparts[:-1]) + '/' + img : {
                tag : fake_layerid
            }
        }

    else:  # when pulling only an img (without repo and registry)
        content = { img : { tag : fake_layerid } }

    with open(imgdir + '/repositories', 'w') as file:
        json.dump(content, file)

    # Create image tar and clean tmp folder
    docker_tar = repo.replace('/', '_') + '_' + img + '.tar'
    sys.stdout.write("Creating archive...")
    sys.stdout.flush()

    tar = tarfile.open(docker_tar, "w")
    tar.add(imgdir, arcname=os.path.sep)
    tar.close()

    shutil.rmtree(imgdir)
    print('\rDocker image pulled: {docker_tar}')
