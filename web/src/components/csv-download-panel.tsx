import { Download, X } from "lucide-react";
import { useRef, useState } from "react";
import {
  CsvDownloadCancelledError,
  computeCsvProgress,
  fetchBarsCsv,
  type BarsQueryParams,
  type CsvProgress,
} from "@/api/endpoints/bars";
import { HttpError } from "@/api/http/types";
import { ErrorState } from "@/components/states";
import { Button } from "@/components/ui/button";
import { Panel, PanelDescription, PanelHeader, PanelTitle } from "@/components/ui/panel";
import { triggerBlobDownload } from "@/lib/content-disposition";
import type { ExportPathDecision } from "@/lib/export-policy";
import { formatBytes } from "@/lib/format";
import { cn } from "@/lib/utils";

type DownloadState =
  | { kind: "idle" }
  | ({ kind: "downloading" } & CsvProgress)
  | {
      kind: "done";
      filename: string;
      bytes: number;
      totalBytes: number | null;
    }
  | { kind: "cancelled" }
  | { kind: "error"; error: HttpError };

export interface CsvDownloadPanelProps {
  /** Applied query (instrument/interval/start/end); null when incomplete. */
  query: BarsQueryParams | null;
  decision: ExportPathDecision;
  className?: string;
}

const RANGE_TOO_LARGE_COPY =
  "原始行超过服务端 JSON 上限（100,000 行），请缩小时间范围后重试。";

/**
 * Direct small-file CSV path (≤31 UTC days).
 *
 * The Blob is passed to a native download anchor unchanged. Percent is shown
 * only with a trusted total; otherwise the UI reports downloaded bytes with an
 * unknown total. It is not streamed to disk and has no Range resume.
 */
export function CsvDownloadPanel({
  query,
  decision,
  className,
}: CsvDownloadPanelProps) {
  const [state, setState] = useState<DownloadState>({ kind: "idle" });
  const controllerRef = useRef<AbortController | null>(null);
  const isDirect = decision.kind === "direct" && query !== null;

  const startDownload = async () => {
    if (!query) {
      return;
    }
    const controller = new AbortController();
    controllerRef.current = controller;
    setState({ kind: "downloading", ...computeCsvProgress(0, null) });
    try {
      const result = await fetchBarsCsv(query, {
        signal: controller.signal,
        onProgress: (progress) => {
          setState({ kind: "downloading", ...progress });
        },
      });
      triggerBlobDownload(result.blob, result.filename);
      setState({
        kind: "done",
        filename: result.filename,
        bytes: result.blob.size,
        totalBytes: result.contentLength,
      });
    } catch (error) {
      if (error instanceof CsvDownloadCancelledError) {
        setState({ kind: "cancelled" });
        return;
      }
      if (error instanceof HttpError) {
        setState({ kind: "error", error });
        return;
      }
      setState({
        kind: "error",
        error: new HttpError({
          kind: "rest",
          status: null,
          code: "csv_download_failed",
          message:
            error instanceof Error ? error.message : "CSV 下载失败",
          details: {},
          requestId: null,
        }),
      });
    } finally {
      controllerRef.current = null;
    }
  };

  const cancel = () => {
    controllerRef.current?.abort();
  };

  let disabledReason: string | null = null;
  if (decision.kind === "job") {
    disabledReason =
      "超过 31 个 UTC 天：只创建 Admin Export Job，不自动退回超大 Blob。";
  } else if (decision.kind === "split") {
    disabledReason = "超过 366 个 UTC 天：需拆分为多个 Export Job。";
  } else if (decision.kind === "invalid") {
    disabledReason = decision.message;
  } else if (query === null) {
    disabledReason = "请先选择 InstrumentId 与 Bar Interval。";
  }

  return (
    <Panel className={cn("mt-6 p-5", className)} aria-label="CSV 直接下载">
      <PanelHeader>
        <div>
          <PanelTitle>小文件 CSV 直接下载</PanelTitle>
          <PanelDescription>
            ≤31 个 UTC 天使用现有 `GET /api/v1/bars?format=csv`，以
            responseType=blob 接收。Blob 是内存缓冲：不支持 Range 续传，也不宣称直接流式写盘；
            服务端列顺序、所有原始 Revision 与精确十进制文本保持不变，前端不会重新序列化 CSV 字节。
          </PanelDescription>
        </div>
        <span className="text-xs text-faint-foreground">
          {decision.message}
        </span>
      </PanelHeader>

      <div className="mt-4 flex flex-wrap items-center gap-2">
        <Button onClick={startDownload} disabled={!isDirect || state.kind === "downloading"}>
          <Download aria-hidden="true" />
          直接下载 CSV（≤31 天）
        </Button>
        {state.kind === "downloading" ? (
          <Button variant="outline" onClick={cancel}>
            <X aria-hidden="true" />
            取消下载
          </Button>
        ) : null}
        {disabledReason ? (
          <span className="text-xs text-faint-foreground">{disabledReason}</span>
        ) : null}
      </div>

      {state.kind === "downloading" ? (
        <div className="mt-3 text-xs" role="status">
          {state.totalBytes !== null && state.percent !== null ? (
            <>
              <progress
                className="h-2 w-full max-w-md"
                max={state.totalBytes}
                value={state.loadedBytes}
              >
                {state.percent}%
              </progress>
              <p className="mt-1 text-muted-foreground">
                已下载 {formatBytes(state.loadedBytes)} /{" "}
                {formatBytes(state.totalBytes)}（{state.percent}%）
              </p>
            </>
          ) : (
            <p className="text-muted-foreground">
              已下载 {formatBytes(state.loadedBytes)} / 总量未知（无可信
              Content-Length，仅显示已接收字节）
            </p>
          )}
        </div>
      ) : null}

      {state.kind === "done" ? (
        <p role="status" className="mt-3 text-xs text-status-good">
          已触发浏览器下载：<span className="font-mono">{state.filename}</span>
          （{formatBytes(state.bytes)}；
          {state.totalBytes !== null
            ? `总量 ${formatBytes(state.totalBytes)}`
            : "总量未知"}
          ）
        </p>
      ) : null}

      {state.kind === "cancelled" ? (
        <p role="status" className="mt-3 text-xs text-muted-foreground">
          下载已取消（AbortController）；未读取完整文件。
        </p>
      ) : null}

      {state.kind === "error" ? (
        <ErrorState
          className="mt-3"
          title="CSV 下载失败"
          description={
            <>
              <div className="font-mono text-[11px]">{state.error.code}</div>
              <div>
                {state.error.code === "range_too_large"
                  ? RANGE_TOO_LARGE_COPY
                  : state.error.message}
              </div>
            </>
          }
          requestId={state.error.requestId}
          onRetry={isDirect ? () => void startDownload() : undefined}
        />
      ) : null}
    </Panel>
  );
}
