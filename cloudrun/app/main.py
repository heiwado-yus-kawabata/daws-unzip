from google.cloud import storage

import csv
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
BUCKET_DST = "heiwado-sandbox-dev-daws"


@app.get(f"/")
def unzip_and_upload(src_zipfile: str):

    time_start = time.time()

    # ZIPファイルのローカルダウンロード
    client = storage.Client(PROJECT)
    bucket = client.bucket(BUCKET_SRC)
    blob = bucket.blob(src_zipfile)

    dst_zipfile = f"zipfile_{str(uuid.uuid4())}.zip"
    with open(dst_zipfile, "wb") as f:
        blob.download_to_file(f)

    zf = zipfile.ZipFile(dst_zipfile, "r")

    # 1ファイルずつunzip
    dst_bucket = client.bucket(BUCKET_DST)
    for src_file in zf.filelist:

        src_file_name = src_file.filename
        zf.extract(src_file_name)

        # tsv→csvへ変換してtmpファイルへ出力
        tmp_file_name = f"tmp_{str(uuid.uuid4())}.csv"

        with open(src_file_name, "r", encoding="CP932") as in_file, open(tmp_file_name, "w", newline="") as out_file:
            tsv_reader = csv.reader(in_file, delimiter='\t')
            csv_writer = csv.writer(out_file, delimiter=',')
            for row in tsv_reader:
                csv_writer.writerow(row)

        # リネーム
        org_file_name = src_file_name.split("/")[1].split(".tsv_")
        dst_file_name = f"{org_file_name[0]}_{org_file_name[1]}.csv"

        # gcsへアップロード
        dst_blob = dst_bucket.blob(dst_file_name)
        dst_blob.upload_from_filename(tmp_file_name)

        # 後片付け
        os.remove(tmp_file_name)
        os.remove(src_file_name)

    time_end = time.time()
    time_process = time_end - time_start
    logging.info(f"{src_zipfile}の処理時間:{time_process}")

    return {"result": "OK"}
