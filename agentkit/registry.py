"""所有 Agent 的單一事實來源：id、埠、上下游關係。

要讓 agent 互相呼叫，位址必須集中一處；Hub 前端、委派迴圈與部署檢查都只讀這裡。

    agent_id   API 埠   DB 埠   說明
    partrag    8000     5434    成品料號相似檢索
    rfq        8300     5435    歷史詢價／成本區間
    bom        8500     —       BOM 結構（不落地，直接查來源系統）
    (hub)      8600     —       Agent_Hub
    car8d      8700     —       客訴單(8D)（不落地，直接查來源系統）
    iso        8800     —       ISO 文件（獨立，不串接）

★ 一律用 127.0.0.1，不要用 localhost。
  Windows 的 localhost 先解析到 IPv6 ::1，而 uvicorn 只綁 IPv4，每個請求都要等
  IPv6 逾時再回退；agent 互打一題可能十幾次，延遲會直接乘上去。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class AgentRef:
    agent_id: str
    name: str
    role: str
    port: int
    project: str
    # 架構圖上的位置。upstream 會呼叫我，downstream 是我可以呼叫的人。
    # 委派迴圈只把 downstream 的工具掛給模型，避免 A->B->A 繞回來。
    downstream: tuple[str, ...] = ()
    upstream: tuple[str, ...] = ()
    # 該 agent 自己的前端路徑（統一 UI 之外，仍然可以單獨開）。None = 沒有原生介面，
    # 純 A2A 服務，統一走 Agent_Hub 的前端。
    native_ui: str | None = None
    # ★ tags 不只是畫面上的標籤：delegate._prefetch_peers() 用它判斷問句是否落在
    #   某個下游的主題上，命中就先打該下游的 /a2a/search 並把資料塞進問句
    #   （見 docs/ARCHITECTURE.md 的「預先查詢」）。所以要寫使用者真的會打的詞，
    #   寫錯時 prefetch 不會觸發，也不會有任何錯誤訊息。
    tags: tuple[str, ...] = field(default_factory=tuple)

    @property
    def base_url(self) -> str:
        # 允許用環境變數改寫，方便日後搬到別台機器而不用改程式
        env = os.environ.get(f"A2A_{self.agent_id.upper()}_URL")
        return env or f"http://127.0.0.1:{self.port}"


AGENTS: tuple[AgentRef, ...] = (
    AgentRef(
        agent_id="bom",
        name="Bom Agent",
        role="依規格建構新產品 BOM：決定要哪些槽位、各多少用量",
        port=8500,
        project="bom-agent",
        # car8d：換料/改版之前要知道這顆料有沒有客訴史
        downstream=("partrag", "car8d"),
        tags=("BOM", "槽位", "用量"),
    ),
    AgentRef(
        agent_id="partrag",
        name="相似搜尋建議 Agent",
        role="成品料號語料的相似料號檢索",
        port=8000,
        project="partrag-agent",
        # 葉節點（downstream 空），多一個上游不會產生環。
        upstream=("bom", "rfq"),
        # tags 照「上游會怎麼問」來挑：命中就由 _prefetch_peers() 先撈資料。
        # 避免放上游幾乎每句都會出現的泛詞，否則每題都會多打一次 search。
        tags=("相似料號", "工程圖", "PLM 欄位"),
    ),
    AgentRef(
        agent_id="rfq",
        name="產品估價 Agent",
        role="RFQ/S/D 歷史紀錄查詢與成本區間參考",
        port=8300,
        project="rfq-agent",
        # car8d：報價前的品質風險。
        # partrag：直接掛上來而不只經 bom 轉問 —— 轉問要多跑一輪下游 LLM 迴圈，
        #   直接掛則「工程圖」tag 會觸發 prefetch，資料在模型開口前就已在問句裡。
        downstream=("bom", "car8d", "partrag"),
        tags=("RFQ", "歷史成本", "合理性檢核"),
    ),
    AgentRef(
        agent_id="car8d",
        name="8D 客訴單 Agent",
        role="客訴抱怨單(8D) 查詢：單號/料號/客戶反查、客訴是否成立、責任歸屬、D3~D8 對策",
        port=8700,
        project="car8d-agent",
        # 葉節點：防環靠「圖是單向的」，沒有下游就不可能繞回上游。
        downstream=(),
        upstream=("bom", "rfq"),
        tags=("客訴", "8D", "CAR", "品質異常", "根本原因"),
    ),
    AgentRef(
        agent_id="iso",
        name="ISO 文件 Agent",
        role="ISO 體系文件（程序/管理辦法/表單）內容與屬性查詢",
        port=8800,
        project="iso-agent",
        # 獨立節點：不呼叫別人、也不讓別人委派。
        downstream=(),
        upstream=(),
        tags=("ISO", "SOP", "管理辦法", "作業規範"),
    ),
)

BY_ID: dict[str, AgentRef] = {a.agent_id: a for a in AGENTS}

HUB_PORT = int(os.environ.get("A2A_HUB_PORT", "8600"))


def get(agent_id: str) -> AgentRef:
    if agent_id not in BY_ID:
        raise KeyError(f"未知的 agent_id：{agent_id}（可用：{', '.join(BY_ID)}）")
    return BY_ID[agent_id]


def peers_of(agent_id: str) -> tuple[AgentRef, ...]:
    """某個 agent 可以呼叫的下游。"""
    return tuple(BY_ID[p] for p in get(agent_id).downstream if p in BY_ID)
