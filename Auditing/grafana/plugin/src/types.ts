export interface PanelOptions {
  title: string;
  backendUrl: string;
  systemPrompt: string;
}

export interface HistoryMessage {
  role: 'user' | 'assistant';
  content: string;
}

export interface AssistantMessage {
  role: 'user' | 'assistant';
  content: string;
  ts: number;
  esQueries?: number;
}

export interface QueryRequest {
  query: string;
  context: {
    dashboardTitle?: string;
    from?: string;
    to?: string;
  };
  systemPrompt?: string;
  history?: HistoryMessage[];
}

export interface QueryResponse {
  answer: string;
  tool_calls_made?: number;
}
