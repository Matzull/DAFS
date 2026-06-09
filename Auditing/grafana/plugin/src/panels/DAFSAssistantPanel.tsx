import React, {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from 'react';
import { createPortal } from 'react-dom';
import { PanelProps } from '@grafana/data';
import { Button, Spinner } from '@grafana/ui';
import { css, cx } from '@emotion/css';
import { marked } from 'marked';
import {
  PanelOptions,
  AssistantMessage,
  HistoryMessage,
  QueryResponse,
} from '../types';

interface Props extends PanelProps<PanelOptions> {}

// ---------------------------------------------------------------------------
// marked — configure once at module level
// ---------------------------------------------------------------------------
marked.use({
  gfm: true,
  breaks: true,
  renderer: {
    link(href: string, title: string | null | undefined, text: string) {
      return `<a href="${href}" target="_blank" rel="noopener noreferrer"${
        title ? ` title="${title}"` : ''
      }>${text}</a>`;
    },
  },
});

// Scoped CSS injected once for markdown rendered inside bubbles
const MD_STYLE_ID = 'dafs-md-styles';
if (!document.getElementById(MD_STYLE_ID)) {
  const s = document.createElement('style');
  s.id = MD_STYLE_ID;
  s.textContent = `
    .dafs-md { font-size: 13px; line-height: 1.6; word-break: break-word; }
    .dafs-md p { margin: 3px 0 6px; }
    .dafs-md h1,.dafs-md h2,.dafs-md h3,.dafs-md h4 { font-weight: 700; margin: 10px 0 4px; }
    .dafs-md h1 { font-size: 1.1em; } .dafs-md h2 { font-size: 1.05em; } .dafs-md h3 { font-size: 1em; }
    .dafs-md strong { font-weight: 700; }
    .dafs-md em { font-style: italic; }
    .dafs-md del { text-decoration: line-through; opacity: 0.7; }
    .dafs-md a { color: #63b3ed; text-decoration: underline; }
    .dafs-md ul { margin: 4px 0; padding-left: 18px; list-style-type: disc; }
    .dafs-md ol { margin: 4px 0; padding-left: 18px; }
    .dafs-md li { margin: 2px 0; }
    .dafs-md code { background: rgba(255,255,255,0.1); padding: 1px 5px; border-radius: 3px; font-family: monospace; font-size: 0.88em; }
    .dafs-md pre { background: rgba(0,0,0,0.35); border: 1px solid rgba(255,255,255,0.1); border-radius: 6px; padding: 10px 12px; overflow-x: auto; margin: 8px 0; }
    .dafs-md pre code { background: none; padding: 0; font-size: 12px; color: #e2e8f0; }
    .dafs-md blockquote { border-left: 3px solid rgba(255,255,255,0.2); margin: 6px 0; padding-left: 10px; opacity: 0.8; font-style: italic; }
    .dafs-md hr { border: none; border-top: 1px solid rgba(255,255,255,0.12); margin: 10px 0; }
    .dafs-md table { border-collapse: collapse; width: 100%; margin: 8px 0; font-size: 12px; }
    .dafs-md th { background: rgba(255,255,255,0.07); border: 1px solid rgba(255,255,255,0.15); padding: 5px 10px; text-align: left; font-weight: 600; white-space: nowrap; }
    .dafs-md td { border: 1px solid rgba(255,255,255,0.1); padding: 4px 10px; }
    .dafs-md tr:nth-child(even) td { background: rgba(255,255,255,0.03); }
    .dafs-md input[type=checkbox] { margin-right: 5px; }
  `;
  document.head.appendChild(s);
}

// ---------------------------------------------------------------------------
// Markdown renderer
// ---------------------------------------------------------------------------
const MarkdownContent: React.FC<{ content: string }> = ({ content }) => {
  const html = useMemo(() => {
    try {
      const raw = marked.parse(content) as string;
      return raw
        .replace(/<table/g, '<div style="overflow-x:auto"><table')
        .replace(/<\/table>/g, '</table></div>');
    } catch {
      return content;
    }
  }, [content]);
  return <div className="dafs-md" dangerouslySetInnerHTML={{ __html: html }} />;
};

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
// Sidebar icon
// ---------------------------------------------------------------------------
const AIIcon: React.FC<{ size?: number }> = ({ size = 14 }) => (
  <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
    <circle cx="12" cy="12" r="3"/>
    <path d="M12 1v4M12 19v4M4.22 4.22l2.83 2.83M16.95 16.95l2.83 2.83M1 12h4M19 12h4M4.22 19.78l2.83-2.83M16.95 7.05l2.83-2.83"/>
  </svg>
);

// ---------------------------------------------------------------------------
// Floating sidebar — rendered via portal into document.body
// ---------------------------------------------------------------------------
interface SidebarProps {
  isOpen: boolean;
  onToggle: () => void;
  title: string;
  sidebarWidth: number;
  messages: AssistantMessage[];
  loading: boolean;
  input: string;
  setInput: React.Dispatch<React.SetStateAction<string>>;
  onSend: (text: string) => void;
  onClear: () => void;
  chatRef: React.RefObject<HTMLDivElement>;
  textareaRef: React.RefObject<HTMLTextAreaElement>;
}

const FloatingSidebar: React.FC<SidebarProps> = ({
  isOpen, onToggle, title, sidebarWidth,
  messages, loading, input, setInput, onSend, onClear,
  chatRef, textareaRef,
}) => {
  const handleKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      onSend(input);
    }
  };

  const s = sidebarStyles(sidebarWidth);

  return (
    <>
      {/* Backdrop — only when open (click to close) */}
      {isOpen && (
        <div
          className={s.backdrop}
          onClick={onToggle}
          aria-hidden
        />
      )}

      {/* Main sidebar panel */}
      <div className={cx(s.sidebar, isOpen ? s.sidebarOpen : s.sidebarClosed)}>
        {/* Header */}
        <div className={s.header}>
          <span className={s.headerTitle}>
            <AIIcon size={14} />
            <span style={{ marginLeft: 6 }}>{title}</span>
          </span>
          <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
            {messages.length > 0 && (
              <button className={s.iconBtn} onClick={onClear} title="Limpiar conversación">
                <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <polyline points="3 6 5 6 21 6"/>
                  <path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/>
                  <path d="M10 11v6M14 11v6"/>
                  <path d="M9 6V4a1 1 0 0 1 1-1h4a1 1 0 0 1 1 1v2"/>
                </svg>
              </button>
            )}
            {/* Collapse button */}
            <button className={s.iconBtn} onClick={onToggle} title="Cerrar panel">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5">
                <path d="M9 18l6-6-6-6"/>
              </svg>
            </button>
          </div>
        </div>

        {/* Chat area */}
        <div className={s.chat} ref={chatRef}>
          {messages.length === 0 && !loading && (
            <div className={s.empty}>
              <svg width="32" height="32" viewBox="0 0 24 24" fill="none"
                   stroke="currentColor" strokeWidth="1.5" style={{ opacity: 0.3, marginBottom: 8 }}>
                <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
              </svg>
              <span>Haz una pregunta sobre el dashboard o los datos de auditoría.</span>
              <span style={{ fontSize: 11, marginTop: 4, opacity: 0.5 }}>
                Enter para enviar · Shift+Enter para salto de línea
              </span>
            </div>
          )}

          {messages.map((msg, idx) => (
            <div
              key={`${msg.ts}-${idx}`}
              className={cx(s.row, msg.role === 'user' ? s.rowUser : s.rowAssistant)}
            >
              {msg.role === 'assistant' && (
                <div className={s.avatar}><AIIcon size={12} /></div>
              )}
              <div className={msg.role === 'user' ? s.bubbleUser : s.bubbleAssistant}>
                <div className={s.bubbleContent}>
                  <MarkdownContent content={msg.content} />
                </div>
                <div className={s.bubbleMeta}>
                  {typeof msg.esQueries === 'number' && msg.esQueries > 0 && (
                    <span className={s.esBadge} title="Consultas ES realizadas">
                      <svg width="9" height="9" viewBox="0 0 24 24" fill="currentColor"
                           style={{ marginRight: 3 }}>
                        <path d="M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm-1 15H9V8h2v9zm4 0h-2V8h2v9z"/>
                      </svg>
                      {msg.esQueries} {msg.esQueries === 1 ? 'consulta ES' : 'consultas ES'}
                    </span>
                  )}
                  <span className={s.timestamp}>
                    {new Date(msg.ts).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                  </span>
                </div>
              </div>
            </div>
          ))}

          {loading && (
            <div className={cx(s.row, s.rowAssistant)}>
              <div className={s.avatar}><AIIcon size={12} /></div>
              <div className={s.bubbleAssistant} style={{ padding: '10px 14px' }}>
                <TypingDots />
              </div>
            </div>
          )}
        </div>

        {/* Composer */}
        <div className={s.composer}>
          <div className={s.inputWrapper}>
            <textarea
              ref={textareaRef}
              className={s.textarea}
              rows={2}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Escribe tu pregunta… (Enter para enviar)"
              disabled={loading}
            />
            <Button
              className={s.sendBtn}
              disabled={loading || !input.trim()}
              onClick={() => onSend(input)}
              size="sm"
              variant="primary"
            >
              {loading ? <Spinner inline size="sm" /> : (
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none"
                     stroke="currentColor" strokeWidth="2.5">
                  <line x1="22" y1="2" x2="11" y2="13"/>
                  <polygon points="22 2 15 22 11 13 2 9 22 2"/>
                </svg>
              )}
            </Button>
          </div>
        </div>
      </div>

      {/* Floating toggle tab — always visible on the right edge */}
      <button
        className={cx(s.toggleTab, isOpen ? s.toggleTabOpen : s.toggleTabClosed)}
        onClick={onToggle}
        title={isOpen ? 'Cerrar asistente' : 'Abrir asistente DAFS AI'}
        aria-label="Toggle DAFS AI Assistant"
      >
        {isOpen ? (
          <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
               stroke="currentColor" strokeWidth="2.5">
            <path d="M9 18l6-6-6-6"/>
          </svg>
        ) : (
          <AIIcon size={16} />
        )}
        {!isOpen && (
          <span style={{
            writingMode: 'vertical-rl', textOrientation: 'mixed',
            fontSize: 11, letterSpacing: '0.05em', marginTop: 6,
            transform: 'rotate(180deg)', fontWeight: 600,
          }}>
            AI
          </span>
        )}
      </button>
    </>
  );
};

// ---------------------------------------------------------------------------
// Panel — portal host + state/logic
// ---------------------------------------------------------------------------
export const DAFSAssistantPanel: React.FC<Props> = ({
  options, timeRange, replaceVariables,
}) => {
  const [isOpen, setIsOpen] = useState(false);
  const [input, setInput] = useState('');
  const [messages, setMessages] = useState<AssistantMessage[]>([]);
  const [loading, setLoading] = useState(false);

  const backendUrl = options.backendUrl || 'http://localhost:8008';
  const systemPrompt = options.systemPrompt || '';
  const title = options.title || 'DAFS AI Assistant';
  const sidebarWidth = 400;

  const chatRef = useRef<HTMLDivElement>(null);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  // Auto-scroll on new messages
  useEffect(() => {
    if (chatRef.current) {
      chatRef.current.scrollTop = chatRef.current.scrollHeight;
    }
  }, [messages, loading]);

  // Focus textarea when sidebar opens
  useEffect(() => {
    if (isOpen) {
      setTimeout(() => textareaRef.current?.focus(), 150);
    }
  }, [isOpen]);

  // ESC to close
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && isOpen) { setIsOpen(false); }
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [isOpen]);

  const buildHistory = useCallback(
    (msgs: AssistantMessage[]): HistoryMessage[] =>
      msgs.map((m) => ({ role: m.role, content: m.content })),
    []
  );

  const sendQuery = useCallback(
    async (text: string) => {
      const trimmed = text.trim();
      if (!trimmed || loading) { return; }

      const historySnapshot = buildHistory(messages);
      const userMsg: AssistantMessage = { role: 'user', content: trimmed, ts: Date.now() };
      setMessages((prev) => [...prev, userMsg]);
      setLoading(true);
      setInput('');

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
    },
    [loading, messages, backendUrl, systemPrompt, timeRange, replaceVariables, buildHistory]
  );

  // The panel itself renders nothing visible — all UI is in the portal
  return createPortal(
    <FloatingSidebar
      isOpen={isOpen}
      onToggle={() => setIsOpen((o) => !o)}
      title={title}
      sidebarWidth={sidebarWidth}
      messages={messages}
      loading={loading}
      input={input}
      setInput={setInput}
      onSend={sendQuery}
      onClear={() => setMessages([])}
      chatRef={chatRef}
      textareaRef={textareaRef}
    />,
    document.body
  );
};

// ---------------------------------------------------------------------------
// Sidebar styles
// ---------------------------------------------------------------------------
const sidebarStyles = (width: number) => ({
  backdrop: css`
    position: fixed;
    inset: 0;
    z-index: 9998;
    background: rgba(0, 0, 0, 0.35);
    animation: fadeIn 0.2s ease;
    @keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
  `,
  sidebar: css`
    position: fixed;
    top: 0;
    right: 0;
    height: 100vh;
    width: ${width}px;
    max-width: 95vw;
    z-index: 9999;
    display: flex;
    flex-direction: column;
    font-size: 13px;
    background: var(--background-primary, #111);
    border-left: 1px solid var(--border-weak, #333);
    box-shadow: -4px 0 24px rgba(0, 0, 0, 0.5);
    transition: transform 0.28s cubic-bezier(0.4, 0, 0.2, 1);
    will-change: transform;
  `,
  sidebarOpen: css`
    transform: translateX(0);
  `,
  sidebarClosed: css`
    transform: translateX(100%);
    pointer-events: none;
  `,
  toggleTab: css`
    position: fixed;
    right: 0;
    top: 50%;
    transform: translateY(-50%);
    z-index: 10000;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 12px 6px;
    background: var(--primary-main, #3d71ab);
    color: #fff;
    border: none;
    border-radius: 8px 0 0 8px;
    cursor: pointer;
    box-shadow: -2px 0 10px rgba(0, 0, 0, 0.4);
    transition: background 0.15s, transform 0.15s, right 0.28s cubic-bezier(0.4, 0, 0.2, 1);
    &:hover {
      background: var(--primary-shade, #2d5a8a);
    }
  `,
  toggleTabOpen: css`
    right: ${width}px;
  `,
  toggleTabClosed: css`
    right: 0;
  `,
  header: css`
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 10px 12px;
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
  `,
  iconBtn: css`
    display: flex;
    align-items: center;
    justify-content: center;
    width: 26px;
    height: 26px;
    background: none;
    border: 1px solid var(--border-weak, #444);
    border-radius: 4px;
    color: var(--text-secondary, #999);
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
    @keyframes fadeIn { from { opacity: 0; transform: translateY(4px); } to { opacity: 1; } }
  `,
  rowUser: css`flex-direction: row-reverse;`,
  rowAssistant: css`flex-direction: row;`,
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
  bubbleContent: css`font-size: 13px; word-break: break-word;`,
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
  timestamp: css`font-size: 10px; color: var(--text-disabled, #666); flex-shrink: 0;`,
  composer: css`
    padding: 8px 12px 10px;
    border-top: 1px solid var(--border-weak, #333);
    background: var(--background-secondary, #1a1a1a);
    flex-shrink: 0;
  `,
  inputWrapper: css`display: flex; gap: 8px; align-items: flex-end;`,
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
    &::placeholder { color: var(--text-disabled, #555); }
    &:disabled { opacity: 0.5; cursor: not-allowed; }
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
