from google.cloud import storage

import csv
import os
import zipfile
import uuid

from zipstream_new import ZipFile

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

    client = storage.Client(PROJECT)
    bucket = client.bucket(BUCKET_SRC)
    zip_blob = storage.blob.Blob(src_zipfile, bucket)
    zip_stream = zip_blob.download_as_text()

    # Zipストリームからファイルを解凍
    unzip_files = ZipFile(file=zip_stream)

    # 解凍したファイルをCloud Storageにストリームとしてアップロード
    for file in unzip_files:
        file_blob = bucket.blob("tsv_output" + os.sep + file.filename)
        file_blob.upload_from_file(file)

    # blob = bucket.blob(src_zipfile)
    #
    # dst_zipfile = f"zipfile_{str(uuid.uuid4())}.zip"
    # with open(dst_zipfile, "wb") as f:
    #     blob.download_to_file(f)
    #
    # # 処理済みファイルリスト
    # file_list = []
    # zf = zipfile.ZipFile(dst_zipfile, "r")
    # for tag in zf.filelist:
    #     file_list.append(tag.filename)
    # zf.close()
    #
    # # 1ファイルずつunzip
    # dst_bucket = client.bucket(BUCKET_DST)
    # for src_file_name in file_list:
    #
    #     zf = zipfile.ZipFile(dst_zipfile, "r")
    #     zf.extract(src_file_name)
    #     zf.close()
    #
    #     # tsv→csvへ変換してtmpファイルへ出力
    #     tmp_file_name = f"tmp_{str(uuid.uuid4())}.csv"
    #
    #     with open(src_file_name, "r", encoding="CP932") as in_file, open(tmp_file_name, "w", newline="") as out_file:
    #         tsv_reader = csv.reader(in_file, delimiter='\t')
    #         csv_writer = csv.writer(out_file, delimiter=',')
    #         for row in tsv_reader:
    #             csv_writer.writerow(row)
    #
    #     # リネーム
    #     org_file_name = src_file_name.split("/")[1].split(".tsv_")
    #     dst_file_name = f"{org_file_name[0]}_{org_file_name[1]}.csv"
    #
    #     # gcsへアップロード
    #     dst_blob = dst_bucket.blob(dst_file_name)
    #     dst_blob.upload_from_filename(tmp_file_name)
    #
    #     # 後片付け
    #     os.remove(tmp_file_name)
    #     os.remove(src_file_name)

    time_end = time.time()
    time_process = time_end - time_start
    logging.info(f"{src_zipfile}の処理時間:{time_process}")

    return {"result": "OK"}
