import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { PanelProps } from '@grafana/data';
import { Button, Spinner } from '@grafana/ui';
import { css, cx } from '@emotion/css';
import { PanelOptions, AssistantMessage, HistoryMessage, QueryResponse } from '../types';

interface Props extends PanelProps<PanelOptions> {}

// ---------------------------------------------------------------------------
// Minimal markdown renderer: code blocks, inline code, bold, bullet lists
// ---------------------------------------------------------------------------
function renderMarkdown(text: string): React.ReactNode[] {
  const nodes: React.ReactNode[] = [];
  // Split on fenced code blocks first
  const parts = text.split(/(```[\s\S]*?```)/g);
  parts.forEach((part, i) => {
    if (part.startsWith('```')) {
      const inner = part.replace(/^```[^\n]*\n?/, '').replace(/```$/, '');
      nodes.push(
        <pre key={i} style={{ background: 'var(--color-bg-secondary,#1a1a2e)', padding: '8px 12px', borderRadius: 4, overflowX: 'auto', fontSize: 11, margin: '6px 0', lineHeight: 1.5 }}>
          <code>{inner}</code>
        </pre>
      );
      return;
    }
    // Process line by line
    const lines = part.split('\n');
    let inList = false;
    lines.forEach((line, li) => {
      const bullet = /^[\-\*]\s+(.+)/.exec(line);
      if (bullet) {
        if (!inList) { inList = true; }
        nodes.push(<li key={`${i}-${li}`} style={{ marginLeft: 16, listStyleType: 'disc' }}>{renderInline(bullet[1])}</li>);
        return;
      }
      if (inList) { inList = false; }
      if (line === '') {
        nodes.push(<br key={`${i}-${li}`} />);
        return;
      }
      nodes.push(<span key={`${i}-${li}`} style={{ display: 'block', marginBottom: 2 }}>{renderInline(line)}</span>);
    });
  });
  return nodes;
}

function renderInline(text: string): React.ReactNode {
  // inline code
  const parts = text.split(/(`[^`]+`)/g);
  return (
    <>
      {parts.map((p, i) =>
        p.startsWith('`') && p.endsWith('`') ? (
          <code key={i} style={{ background: 'rgba(255,255,255,0.1)', padding: '1px 4px', borderRadius: 3, fontSize: '0.9em' }}>
            {p.slice(1, -1)}
          </code>
        ) : (
          // bold
          <span key={i} dangerouslySetInnerHTML={{ __html: p.replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>').replace(/\*(.+?)\*/g, '<em>$1</em>') }} />
        )
      )}
    </>
  );
}

// ---------------------------------------------------------------------------
// Typing indicator
// ---------------------------------------------------------------------------
const TypingDots: React.FC = () => (
  <span style={{ display: 'inline-flex', gap: 3, alignItems: 'center', height: 16 }}>
    {[0, 1, 2].map((i) => (
      <span
        key={i}
        style={{
          width: 6, height: 6, borderRadius: '50%',
          background: 'var(--text-secondary,#aaa)',
          animation: `bounce 1.2s ${i * 0.2}s infinite`,
        }}
      />
    ))}
    <style>{`@keyframes bounce { 0%,80%,100%{transform:translateY(0)} 40%{transform:translateY(-5px)} }`}</style>
  </span>
);

// ---------------------------------------------------------------------------
// Panel
// ---------------------------------------------------------------------------
export const DAFSAssistantPanel: React.FC<Props> = ({ options, width, height, timeRange, replaceVariables }) => {
  const [input, setInput] = useState('');
  const [messages, setMessages] = useState<AssistantMessage[]>([]);
  const [loading, setLoading] = useState(false);

  const backendUrl = options.backendUrl || 'http://localhost:8008';
  const systemPrompt = options.systemPrompt || '';
  const title = options.title || 'DAFS AI Assistant';

  const chatRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Auto-scroll to bottom on new messages
  useEffect(() => {
    if (chatRef.current) {
      chatRef.current.scrollTop = chatRef.current.scrollHeight;
    }
  }, [messages, loading]);

  const buildHistory = useCallback((msgs: AssistantMessage[]): HistoryMessage[] =>
    msgs.map((m) => ({ role: m.role, content: m.content })),
    []
  );

  const sendQuery = useCallback(async (text: string) => {
    const trimmed = text.trim();
    if (!trimmed || loading) { return; }

    const userMsg: AssistantMessage = { role: 'user', content: trimmed, ts: Date.now() };
    setMessages((prev) => {
      const next = [...prev, userMsg];
      return next;
    });
    setLoading(true);
    setInput('');

    // Capture history before adding the new user message (so we pass previous turns)
    const historySnapshot = buildHistory(messages);

    try {
      const response = await fetch(`${backendUrl}/query`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query: replaceVariables(trimmed),
          context: {
            dashboardTitle: document.title,
            from: timeRange.from.toISOString(),
            to: timeRange.to.toISOString(),
          },
          systemPrompt,
          history: historySnapshot,
        }),
      });

      if (!response.ok) {
        const body = await response.text().catch(() => '');
        throw new Error(`HTTP ${response.status}${body ? ': ' + body.slice(0, 200) : ''}`);
      }

      const data = (await response.json()) as QueryResponse;
      setMessages((prev) => [
        ...prev,
        {
          role: 'assistant',
          content: data.answer,
          ts: Date.now(),
          esQueries: data.tool_calls_made,
        },
      ]);
    } catch (e) {
      const msg = e instanceof Error ? e.message : String(e);
      setMessages((prev) => [
        ...prev,
        { role: 'assistant', content: `⚠️ Error: ${msg}`, ts: Date.now() },
      ]);
    } finally {
      setLoading(false);
      setTimeout(() => textareaRef.current?.focus(), 50);
    }
  }, [loading, messages, backendUrl, systemPrompt, timeRange, replaceVariables, buildHistory]);

  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      sendQuery(input);
    }
  };

  const styles = useMemo(() => getStyles({ width, height }), [width, height]);

  return (
    <div className={styles.container}>
      {/* Header */}
      <div className={styles.header}>
        <span className={styles.headerTitle}>
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" style={{ marginRight: 6, flexShrink: 0 }}>
            <circle cx="12" cy="12" r="3"/><path d="M12 1v4M12 19v4M4.22 4.22l2.83 2.83M16.95 16.95l2.83 2.83M1 12h4M19 12h4M4.22 19.78l2.83-2.83M16.95 7.05l2.83-2.83"/>
          </svg>
          {title}
        </span>
        {messages.length > 0 && (
          <button
            className={styles.clearBtn}
            onClick={() => setMessages([])}
            title="Limpiar conversación"
          >
            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <polyline points="3 6 5 6 21 6"/><path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/><path d="M10 11v6M14 11v6"/><path d="M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"/>
            </svg>
            Limpiar
          </button>
        )}
      </div>

      {/* Chat area */}
      <div className={styles.chat} ref={chatRef}>
        {messages.length === 0 && !loading && (
          <div className={styles.empty}>
            <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" style={{ opacity: 0.3, marginBottom: 8 }}>
              <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
            </svg>
            <span>Haz una pregunta sobre el dashboard o los datos de auditoría.</span>
            <span style={{ fontSize: 11, marginTop: 4, opacity: 0.5 }}>Enter para enviar · Shift+Enter para salto de línea</span>
          </div>
        )}

        {messages.map((msg, idx) => (
          <div
            key={`${msg.ts}-${idx}`}
            className={cx(styles.row, msg.role === 'user' ? styles.rowUser : styles.rowAssistant)}
          >
            {msg.role === 'assistant' && (
              <div className={styles.avatar}>
                <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor">
                  <circle cx="12" cy="12" r="3"/><path d="M12 1v4M12 19v4M4.22 4.22l2.83 2.83M16.95 16.95l2.83 2.83M1 12h4M19 12h4M4.22 19.78l2.83-2.83M16.95 7.05l2.83-2.83"/>
                </svg>
              </div>
            )}
            <div className={msg.role === 'user' ? styles.bubbleUser : styles.bubbleAssistant}>
              <div className={styles.bubbleContent}>{renderMarkdown(msg.content)}</div>
              <div className={styles.bubbleMeta}>
                {typeof msg.esQueries === 'number' && msg.esQueries > 0 && (
                  <span className={styles.esBadge} title="Consultas Elasticsearch realizadas">
                    <svg width="9" height="9" viewBox="0 0 24 24" fill="currentColor" style={{ marginRight: 3 }}>
                      <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 15H9V8h2v9zm4 0h-2V8h2v9z"/>
                    </svg>
                    {msg.esQueries} {msg.esQueries === 1 ? 'consulta ES' : 'consultas ES'}
                  </span>
                )}
                <span className={styles.timestamp}>{new Date(msg.ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}</span>
              </div>
            </div>
          </div>
        ))}

        {loading && (
          <div className={cx(styles.row, styles.rowAssistant)}>
            <div className={styles.avatar}>
              <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor">
                <circle cx="12" cy="12" r="3"/><path d="M12 1v4M12 19v4M4.22 4.22l2.83 2.83M16.95 16.95l2.83 2.83M1 12h4M19 12h4M4.22 19.78l2.83-2.83M16.95 7.05l2.83-2.83"/>
              </svg>
            </div>
            <div className={styles.bubbleAssistant} style={{ padding: '10px 14px' }}>
              <TypingDots />
            </div>
          </div>
        )}
      </div>

      {/* Composer */}
      <div className={styles.composer}>
        <div className={styles.inputWrapper}>
          <textarea
            ref={textareaRef}
            className={styles.textarea}
            rows={2}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Escribe tu pregunta… (Enter para enviar)"
            disabled={loading}
          />
          <Button
            className={styles.sendBtn}
            disabled={loading || !input.trim()}
            onClick={() => sendQuery(input)}
            size="sm"
            variant="primary"
          >
            {loading ? <Spinner inline size="sm" /> : (
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                <line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/>
              </svg>
            )}
          </Button>
        </div>
      </div>
    </div>
  );
};

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------
const getStyles = ({ width, height }: { width: number; height: number }) => ({
  container: css`
    width: ${width}px;
    height: ${height}px;
    display: flex;
    flex-direction: column;
    font-size: 13px;
    background: var(--background-primary, #111);
    overflow: hidden;
  `,
  header: css`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 8px 12px;
    border-bottom: 1px solid var(--border-weak, #333);
    background: var(--background-secondary, #1a1a1a);
    flex-shrink: 0;
  `,
  headerTitle: css`
    font-weight: 600;
    font-size: 13px;
    display: flex;
    align-items: center;
    color: var(--text-primary, #d8d9da);
    letter-spacing: 0.01em;
  `,
  clearBtn: css`
    display: flex;
    align-items: center;
    gap: 4px;
    background: none;
    border: 1px solid var(--border-weak, #444);
    border-radius: 4px;
    color: var(--text-secondary, #999);
    font-size: 11px;
    padding: 3px 8px;
    cursor: pointer;
    &:hover {
      background: var(--background-secondary, #252525);
      color: var(--text-primary, #ddd);
      border-color: var(--border-medium, #666);
    }
  `,
  chat: css`
    flex: 1;
    overflow-y: auto;
    padding: 12px 12px 4px;
    display: flex;
    flex-direction: column;
    gap: 12px;
    scroll-behavior: smooth;
    &::-webkit-scrollbar { width: 4px; }
    &::-webkit-scrollbar-track { background: transparent; }
    &::-webkit-scrollbar-thumb { background: var(--border-medium, #444); border-radius: 4px; }
  `,
  empty: css`
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    color: var(--text-secondary, #888);
    font-size: 12px;
    text-align: center;
    gap: 2px;
    padding: 24px;
  `,
  row: css`
    display: flex;
    align-items: flex-end;
    gap: 8px;
    animation: fadeIn 0.15s ease-in;
    @keyframes fadeIn { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; transform: translateY(0); } }
  `,
  rowUser: css`
    flex-direction: row-reverse;
  `,
  rowAssistant: css`
    flex-direction: row;
  `,
  avatar: css`
    width: 24px;
    height: 24px;
    border-radius: 50%;
    background: var(--primary-shade, #1e6091);
    display: flex;
    align-items: center;
    justify-content: center;
    flex-shrink: 0;
    color: #fff;
  `,
  bubbleUser: css`
    max-width: 80%;
    background: var(--primary-main, #3d71ab);
    color: #fff;
    border-radius: 14px 14px 4px 14px;
    padding: 8px 12px;
    line-height: 1.5;
    box-shadow: 0 1px 3px rgba(0,0,0,0.2);
  `,
  bubbleAssistant: css`
    max-width: 85%;
    background: var(--background-secondary, #1f2128);
    border: 1px solid var(--border-weak, #333);
    border-radius: 14px 14px 14px 4px;
    padding: 8px 12px;
    line-height: 1.5;
    box-shadow: 0 1px 3px rgba(0,0,0,0.15);
    color: var(--text-primary, #d8d9da);
  `,
  bubbleContent: css`
    font-size: 13px;
    word-break: break-word;
  `,
  bubbleMeta: css`
    display: flex;
    align-items: center;
    gap: 8px;
    margin-top: 6px;
    justify-content: flex-end;
  `,
  esBadge: css`
    display: inline-flex;
    align-items: center;
    font-size: 10px;
    color: var(--text-secondary, #aaa);
    background: rgba(255,255,255,0.06);
    border: 1px solid var(--border-weak, #444);
    border-radius: 10px;
    padding: 1px 6px;
  `,
  timestamp: css`
    font-size: 10px;
    color: var(--text-disabled, #666);
    flex-shrink: 0;
  `,
  composer: css`
    padding: 8px 12px 10px;
    border-top: 1px solid var(--border-weak, #333);
    background: var(--background-secondary, #1a1a1a);
    flex-shrink: 0;
  `,
  inputWrapper: css`
    display: flex;
    gap: 8px;
    align-items: flex-end;
  `,
  textarea: css`
    flex: 1;
    resize: none;
    background: var(--background-primary, #111);
    border: 1px solid var(--border-medium, #444);
    border-radius: 8px;
    color: var(--text-primary, #d8d9da);
    font-size: 13px;
    padding: 8px 10px;
    line-height: 1.5;
    outline: none;
    font-family: inherit;
    &:focus {
      border-color: var(--primary-shade, #3d71ab);
      box-shadow: 0 0 0 2px rgba(61,113,171,0.2);
    }
    &::placeholder {
      color: var(--text-disabled, #555);
    }
    &:disabled {
      opacity: 0.5;
      cursor: not-allowed;
    }
  `,
  sendBtn: css`
    flex-shrink: 0;
    height: 36px;
    width: 36px;
    padding: 0 !important;
    display: flex;
    align-items: center;
    justify-content: center;
    border-radius: 8px !important;
  `,
});
