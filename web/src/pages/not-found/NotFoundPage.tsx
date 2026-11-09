import { Link } from "react-router-dom";
import { PageHeader } from "@/components/page-header";
import { EmptyState } from "@/components/states";
import { Panel } from "@/components/ui/panel";

export function NotFoundPage() {
  return (
    <>
      <PageHeader title="未找到页面" description="请求的路由不存在。" />
      <Panel className="mt-6 p-5">
        <EmptyState
          title="404"
          description="请从顶部导航进入五个一级模块之一。"
          action={
            <Link className="text-xs underline underline-offset-2" to="/monitoring">
              返回监控
            </Link>
          }
        />
      </Panel>
    </>
  );
}
