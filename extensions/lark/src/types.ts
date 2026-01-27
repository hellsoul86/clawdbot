export type LarkRuntimeEnv = {
  log?: (message: string) => void;
  error?: (message: string) => void;
};

export type LarkMention = {
  id?: {
    union_id?: string;
    user_id?: string;
    open_id?: string;
  };
  name: string;
};

export type LarkMessageEvent = {
  tenant_key?: string;
  sender: {
    sender_id?: {
      union_id?: string;
      user_id?: string;
      open_id?: string;
    };
    sender_type: string;
  };
  message: {
    message_id: string;
    chat_id: string;
    chat_type: string;
    message_type: string;
    content: string;
    create_time?: string;
    thread_id?: string;
    root_id?: string;
    mentions?: LarkMention[];
  };
};
