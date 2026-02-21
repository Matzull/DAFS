import { PanelPlugin } from '@grafana/data';
import { DAFSAssistantPanel } from './panels/DAFSAssistantPanel';
import { PanelOptions } from './types';

export const plugin = new PanelPlugin<PanelOptions>(DAFSAssistantPanel).setPanelOptions((builder) => {
  return builder
    .addTextInput({
      path: 'title',
      name: 'Panel title',
      defaultValue: 'DAFS AI Assistant',
    })
    .addTextInput({
      path: 'backendUrl',
      name: 'Backend URL',
      description: 'Ejemplo: http://localhost:8008',
      defaultValue: 'http://localhost:8008',
    })
    .addTextInput({
      path: 'systemPrompt',
      name: 'System prompt',
      defaultValue: 'Eres un asistente de observabilidad para dashboards de Grafana.',
    });
});
