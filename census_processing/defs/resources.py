import json
from collections.abc import Sequence

import httpx
import pandas as pd
import requests
import websockets

import dagster as dg


def _get_logger(context: dg.InitResourceContext) -> dg.Logger:
    logger = context.log
    if logger is None:
        err = "Context log is not available. Ensure this function is called within a Dagster resource or op context."
        raise RuntimeError(err)
    return logger


class PathResource(dg.ConfigurableResource):
    data_path: str


class LyraResource(dg.ConfigurableResource):
    host: str
    headers: dict[str, str]

    def request(self, endpoint: str, cvegeos: Sequence[str]) -> pd.Series:
        url = f"https://{self.host}/{endpoint}/cvegeo"
        response = requests.post(
            url, headers=self.headers, json={"cvegeo": list(cvegeos)}, timeout=100
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as e:
            err = f"Request to {url} failed with status code {response.status_code}: {response.text}"
            raise RuntimeError(err) from e

        return pd.Series(response.json())

    async def download_data(self, download_id: str) -> dict:
        async with httpx.AsyncClient(headers=self.headers) as client:
            response = await client.get(
                f"https://{self.host}/download-result/{download_id}", timeout=100
            )
            response.raise_for_status()
            return response.json()

    async def request_websocket(
        self, context: dg.InitResourceContext, endpoint: str, cvegeos: Sequence[str]
    ) -> dict:
        logger = _get_logger(context)

        url = f"wss://{self.host}/ws/{endpoint}/cvegeo"
        async with websockets.connect(url) as websocket:
            request_payload = {"function_name": "tree_coverage", "geojson": test}
            await websocket.send(json.dumps(request_payload))

            ack_str = await websocket.recv()
            ack = json.loads(ack_str)

            msg = f"Server acknowledged. Task ID: {ack.get('task_id')}"
            logger.info(msg)

            notification_str = await websocket.recv()
            notification = json.loads(notification_str)
            status = notification["status"]

        if status == "error":
            msg = f"Worker failed: {notification.get('message')}"
            raise RuntimeError(msg)

        if status == "success":
            download_id = notification.get("download_id")

            msg = f"Worker finished. Received download ticket: {download_id}"
            logger.info(msg)

            return await self.download_data(download_id)

        err = f"Unexpected status received from worker: {status}"
        raise RuntimeError(err)
