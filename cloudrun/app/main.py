from google.cloud import storage

import os
import zipfile
import uuid

import google.cloud.logging
import logging
import time

from fastapi import FastAPI

# APIサーバ起動
app = FastAPI()
# ロガー設定
google.cloud.logging.Client().setup_logging()

PROJECT = "heiwado-sandbox-dev"
BUCKET_SRC = "heiwado-sandbox-dev-daws-from"

@app.get(f"/")
def unzip_and_upload(src_zipfile: str):

    time_start = time.time()

    client = storage.Client(PROJECT)
    bucket = client.bucket(BUCKET_SRC)
    blob = bucket.blob(src_zipfile)

    dst_zipfile = f"zipfile_{str(uuid.uuid4())}.zip"
    with open(dst_zipfile, "wb") as f:
        blob.download_to_file(f)

    # 1ファイルずつunzip
    zf = zipfile.ZipFile(dst_zipfile, "r")
    for src_file in zf.filelist:

        src_file_name = src_file.filename
        zf.extract(src_file_name)

        # gcsへアップロード
        dst_blob = bucket.blob(src_file_name)
        dst_blob.upload_from_filename({src_file_name})

        # 後片付け
        os.remove(src_file_name)

    os.remove(dst_zipfile)
    time_end = time.time()
    time_process = time_end - time_start
    logging.info(f"{src_zipfile}の処理時間:{time_process}")

    return {"result": "OK"}
