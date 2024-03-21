import re
import json
import itertools
from typing import Optional, Dict, List, Tuple
import textwrap
import numpy as np
import pandas as pd
from langchain.text_splitter import (
    Language,
    RecursiveCharacterTextSplitter,
)


def get_completed_prompts(base_template: str, df: pd.DataFrame) -> Tuple[List[str], np.ndarray]:
    """
        Helper method that produces formatted prompts given a template and data in a Pandas DataFrame.
        It also returns the ID of any empty templates that failed to be filled due to missing data.
        
        :param base_template: string with placeholders for each column in the DataFrame. Placeholders should follow double curly braces format, e.g. `{{column_name}}`. All placeholders should have matching columns in `df`.
        :param df: pd.DataFrame to generate full prompts. Each placeholder in `base_template` must exist as a column in the DataFrame. If a column is not in the template, it is ignored entirely.
        
        :return prompts: list of in-filled prompts using `base_template` and relevant columns from `df`
        :return empty_prompt_ids: np.int numpy array (shape (n_missing_rows,)) with the row indexes where in-fill failed due to missing data.
    """  # noqa
    columns = []
    spans = []
    matches = list(re.finditer("{{(.*?)}}", base_template))

    assert len(matches) > 0, 'No placeholders found in the prompt, please provide a valid prompt template.'

    first_span = matches[0].start()
    last_span = matches[-1].end()

    for m in matches:
        columns.append(m[0].replace('{', '').replace('}', ''))
        spans.extend((m.start(), m.end()))

    spans = spans[1:-1]  # omit first and last, they are added separately
    template = [base_template[s:e] for s, e in
                list(zip(spans, spans[1:]))[::2]]  # take every other to skip placeholders  # noqa
    template.insert(0, base_template[0:first_span])  # add prompt start
    template.append(base_template[last_span:])  # add prompt end

    empty_prompt_ids = np.where(df[columns].isna().all(axis=1).values)[0]

    df['__mdb_prompt'] = ''
    for i in range(len(template)):
        atom = template[i]
        if i < len(columns):
            col = df[columns[i]].replace(to_replace=[None], value='')  # add empty quote if data is missing
            df['__mdb_prompt'] = df['__mdb_prompt'].apply(lambda x: x + atom) + col.astype("string")
        else:
            df['__mdb_prompt'] = df['__mdb_prompt'].apply(lambda x: x + atom)
    prompts = list(df['__mdb_prompt'])

    return prompts, empty_prompt_ids


def ft_jsonl_validation(
        items: list,  # read from a JSONL file
        messages_col: str = "messages",

        # valid keys for each chat message
        role_key: str = "role",
        content_key: str = "content",
        name_key: str = "name",

        # valid roles for each chat message
        system_key: str = "system",
        user_key: str = "user",
        assistant_key: str = "assistant",
):
    """
    This helper checks a list of dictionaries for compliance with the format usually expected by LLM providers
    (such as OpenAI or AnyscaleEndpoints) for fine-tuning LLMs that generate chat completions.
    
    Defaults are set according to the expected format, but these can be changed if needed by any given provider.
    
    :param items: list of JSON lines, each dictionary containing a chat sequence. Should be read from a JSONL file.
    :param messages_col: key in each dictionary to access a sequence of chat messages
    
    
    For chat-level checks, this method defers to `ft_chat_format_validation()` below. Relevant parameters for it are:
    
    For each chat:
    :param role_key: key that defines the role of each message (e.g. system, user, or LLM)
    :param content_key: key that defines the content of each message
    :param name_key: key that defines the name of each message
    
    For each message:
    :param system_key: valid role for each chat message
    :param user_key: valid role for each chat message
    :param assistant_key: valid role for each chat message
    
    :return: None, raises an Exception if validation fails.
    """  # noqa
    try:
        if not all([isinstance(m, dict) for m in items]):
            raise Exception("Each line in the provided data should be a dictionary")

        for line_num, batch in enumerate(items):
            prefix = f"error in chat #{line_num + 1}, "

            if not isinstance(batch[messages_col], list):
                raise Exception(
                    f"{prefix}Each line in the provided data should have a '{messages_col}' key with a list of messages")  # noqa

            if messages_col not in batch:
                raise Exception(f"{prefix}Each line in the provided data should have a '{messages_col}' key")

            messages = batch[messages_col]
            try:
                ft_chat_format_validation(
                    messages,
                    role_key=role_key,
                    content_key=content_key,
                    name_key=name_key,
                    system_key=system_key,
                    user_key=user_key,
                    assistant_key=assistant_key,
                )
            except Exception as e:
                raise Exception(f"{prefix}{e}")

    except Exception as e:
        raise Exception(f"Fine-tuning data format is not valid. Got {e}")


def ft_chat_format_validation(
        chat: list,
        transitions: Optional[Dict] = None,
        system_key: str = "system",
        user_key: str = "user",
        assistant_key: str = "assistant",
        role_key: str = "role",
        content_key: str = "content",
        name_key: str = "name",
):
    """
    Finite state machine to check a chat has valid format to finetune an LLM with it.
    Follows OpenAI ChatCompletion format (also used by other providers such as AnyscaleEndpoints).
    Reference: https://cookbook.openai.com/examples/chat_finetuning_data_prep

    The unit test in `test_llm_utils.py` for examples of valid and invalid chats.
    
    :param chat: list of dictionaries, each containing a chat message
    :param transitions: optional dictionary defining valid transitions between chat messages (e.g. from system to user to assistant)
    
    For each chat:
    :param role_key: key that defines the role of each message (e.g. system, user, or LLM)
    :param content_key: key that defines the content of each message
    :param name_key: key that defines the name of each message
    
    For each message:
    :param system_key: valid role for each chat message
    :param user_key: valid role for each chat message
    :param assistant_key: valid role for each chat message
    
    :return: None if chat is valid, otherwise raise an informative Exception.
    """  # noqa

    valid_keys = (role_key, content_key, name_key)
    valid_roles = (system_key, user_key, assistant_key)

    for c in chat:
        if any(k not in valid_keys for k in c.keys()):
            raise Exception(f"Each message should only have these keys: `{valid_keys}`. Found: `{c.keys()}`")

    roles = [m[role_key] for m in chat]
    contents = [m[content_key] for m in chat]

    if len(roles) != len(contents):
        raise Exception(f"Each message should contain both `{role_key}` and `{content_key}` fields")

    if len(roles) == 0:
        raise Exception('Chat should have at least one message')

    if assistant_key not in roles:
        raise Exception('Chat should have at least one assistant message')  # otherwise it is useless for FT

    if user_key not in roles:
        raise Exception('Chat should have at least one user message')  # perhaps remove in the future

    # set default transitions for finite state machine if undefined
    if transitions is None:
        transitions = {
            None: [system_key, user_key],
            system_key: [user_key],
            user_key: [assistant_key],
            assistant_key: [user_key]
        }

    # check order is valid via finite state machine
    state = None
    for i, (role, content) in enumerate(zip(roles, contents)):

        prefix = f"message #{i + 1}: "

        # check invalid roles
        if role not in valid_roles:
            raise Exception(f"{prefix}Invalid role (found `{role}`, expected one of `{valid_roles}`)")

        # check content
        if not isinstance(content, str):
            raise Exception(f"{prefix}Content should be a string, got type `{type(content)}`")

        # check transition
        if role not in transitions[state]:
            raise Exception(f"{prefix}Invalid transition from `{state}` to `{role}`")
        else:
            state = role


def ft_formatter(df: pd.DataFrame) -> List[Dict]:
    """
    Data preparation entry point for chat LLM finetuning. This method will dispatch to the appropriate formatters.

    Supported formats:
        - code: long tabular format with a `code` column
        - chat: long tabular format with `role` and `content` columns, or a JSON format with a `chat_json` column.
    """
    if 'code' in df.columns:
        df = ft_code_formatter(df)

    elif {'question', 'context', 'answer'}.issubset(set(df.columns)):
        # TODO: handler user-specified names for these columns
        df = ft_cqa_formatter(df)

    return ft_chat_formatter(df)


def ft_chat_formatter(df: pd.DataFrame) -> List[Dict]:
    """
        For more details, check `FineTuning -> Data Format` in the Anyscale API reference, or the OpenAI equivalent.
        Additionally, the unit test in `test_llm_utils.py` provides example usage.

        :param df: input dataframe has chats in one of the following formats:
            1) long tabular: at least two columns, `role` and `content`. Rows contain >= 1 chats in long (stacked) format.

            2) JSON: at least one column, `chat_json`. Each row contains exactly 1 chat in JSON format.
                Example for `chat_json` content: 
                    > `{"messages": [{"role": "user", "content": "Hello!"}, {"role": "assistant", "content": "Hi!"}]}`

        Optional df columns are:
            - chat_id: unique identifier for each chat
            - message_id: unique identifier for each message within each chat

            Data will be sorted by both if they are provided. 

            If only `chat_id` is provided, data will be sorted by it with a stable sort, so messages for each chat 
            will be in the same order as in the original data.

            If only `message_id` is provided, it must not contain duplicate IDs. Entire dataset will be treated 
            as a single chat. Otherwise an exception will be raised.
            
        :return: list of chats. Each chat is a dictionary with a top level key 'messages' containing a list of messages 
        that comply with the OpenAI's ChatEndpoint expected format (i.e., each is a dictionary with a `role` and 
        `content` key.

    """  # noqa
    # 1. pre-sort df on optional columns
    if 'chat_id' in df.columns:
        if 'message_id' in df.columns:
            df = df.sort_values(['chat_id', 'message_id'])
        else:
            df = df.sort_values(['chat_id'], kind='stable')
    elif 'message_id' in df.columns:
        if df['message_id'].duplicated().any():
            raise Exception("If `message_id` is provided, it must not contain duplicate IDs.")
        df = df.sort_values(['message_id'])

    # 2. build chats
    chats = []

    # 2a. chats are in JSON format
    if 'chat_json' in df.columns:
        for _, row in df.iterrows():
            try:
                chat = json.loads(row['chat_json'])
                assert list(chat.keys()) == ['messages'], "Each chat should have a 'messages' key, and nothing else."
                ft_chat_format_validation(chat['messages'])  # will raise Exception if chat is invalid
                chats.append(chat)
            except json.JSONDecodeError:
                pass  # TODO: add logger info here, prompt user to clean dataset carefully

    # 2b. chats are in tabular format - aggregate each chat sequence into one row
    else:
        chat = []
        for i, row in df.iterrows():
            if row['role'] == 'system' and len(chat) > 0:
                ft_chat_format_validation(chat)  # will raise Exception if chat is invalid
                chats.append({'messages': chat})
                chat = []
            event = {'role': row['role'], 'content': row['content']}
            chat.append(event)

        ft_chat_format_validation(chat)  # will raise Exception if chat is invalid
        chats.append({'messages': chat})

    return chats



def validate_args(args, required_keys, keys_collection, extra_keys):
    ''' args: The dictionary containing the arguments to be validated.
        required_keys: A list of keys that are required for the handler.
        keys_collection: A list of sets of keys, where only one key from each set can be present in the args.
    '''
    if 'using' not in args:
        raise Exception(
            "Handler requires a USING clause! Refer to its documentation for more details."
        )
    else:
        args = args['using']

    # Check if at least one of the required keys is present
    if not any(key in args for key in required_keys):
        raise Exception(
            f"At least one of {', '.join(required_keys)} is required for this handler."
        )

    # Check if exclusive sets of keys are mutually exclusive
    for keys in keys_collection:
        if keys[0] in args and any(
                x[0] in args for x in keys_collection if x != keys
            ):
            raise Exception(
                textwrap.dedent(
                    f"""\
                    Please provide only one of the following key sets:
                    {', '.join(str(keys))}
                """
                )
            )

    # Check for unknown arguments
    known_args = set(required_keys) | extra_keys
    known_args = known_args.union(*keys_collection)

    unknown_args = set(args.keys()) - known_args
    if unknown_args:
        raise Exception(
            f"Unknown arguments: {', '.join(unknown_args)}.\n Known arguments are: {', '.join(known_args)}"
        )


# overide_pred_args_over_args_check supported mode
def pred_time_args(args, pred_args, supported_modes):
    
    if pred_args.get('mode'):
        if pred_args['mode'] in supported_modes:
            args['mode'] = pred_args['mode']
        else:
            raise Exception(
                f"Invalid operation mode. Please use one of {supported_modes}."
            )
    args.update(pred_args)
    return args

def generate_llm_prompts(df, args, base_template, json_prompt):
    if base_template:
        prompts, empty_prompt_ids = get_completed_prompts(base_template=base_template, df=df)
    elif args.get('context_column', False):
        prompts, empty_prompt_ids = generate_context_prompts(df, args)
    elif args.get('json_struct', False):
        prompts, empty_prompt_ids = generate_json_prompts(df, args, json_prompt)
        
    elif 'prompt' in args:
        prompts, empty_prompt_ids = generate_prompt_column(df, args)
    else:
        prompts, empty_prompt_ids = generate_default_prompts(df, args)
    prompts = [j for i, j in enumerate(prompts) if i not in empty_prompt_ids]
    return prompts, empty_prompt_ids

def generate_context_prompts(df, args):
    empty_prompt_ids = np.where(
        df[[args['context_column'], args['question_column']]]
        .isna()
        .all(axis=1)
        .values
    )[0]
    contexts = list(df[args['context_column']].apply(lambda x: str(x)))
    questions = list(df[args['question_column']].apply(lambda x: str(x)))
    prompts = [
        f'Give only answer for: \nContext: {c}\nQuestion: {q}\nAnswer: '
        for c, q in zip(contexts, questions)
    ]
    return prompts, empty_prompt_ids

def generate_json_prompts(df, args, json_prompt):
    empty_prompt_ids = np.where(
        df[[args['input_text']]].isna().all(axis=1).values
    )[0]
    prompts = []
    for i in df.index:
        if 'json_struct' in df.columns:
            if isinstance(df['json_struct'][i], str):
                df['json_struct'][i] = json.loads(df['json_struct'][i])
            json_struct = ''
            for ind, val in enumerate(df['json_struct'][i].values()):
                json_struct = json_struct + f'{ind}. {val}\n'
        else:
            json_struct = ''
            for ind, val in enumerate(args['json_struct'].values()):
                json_struct = json_struct + f'{ind + 1}. {val}\n'

        p = json_prompt.replace('{{json_struct}}', json_struct)
        for column in df.columns:
            if column == 'json_struct':
                continue
            p = p.replace(f'{{{{{column}}}}}', str(df[column][i]))
        prompts.append(p)
    return prompts, empty_prompt_ids

def generate_prompt_column(df, args):
    empty_prompt_ids = []
    prompts = list(df[args['user_column']])
    return prompts, empty_prompt_ids

def generate_default_prompts(df, args):
    empty_prompt_ids = np.where(
        df[[args['question_column']]].isna().all(axis=1).values
    )[0]
    prompts = list(df[args['question_column']].apply(lambda x: str(x)))
    return prompts, empty_prompt_ids

def ft_code_formatter(
        df: pd.DataFrame,
        format='chat',
        language='python',
        chunk_size=100,
        chunk_overlap=0,
        chat_sections=('Code prefix', 'Code suffix', 'Completion'),
        fim_tokens=('<PRE>', '<SUF>', '<MID>'),
) -> pd.DataFrame:
    """
    This utility processes a raw codebase stored as a dataframe with a `code` column, where
    every row may be an entire file or some portion of it.
    It chunks code into triples made of a prefix, middle, and suffix.

    Depending on the target LLM, these triples are then formatted into a chat-like prompt, or a
    fill-in-the-middle (FIM) prompt. The latter is used for fine-tuning models like codellama,
    while the former is more generic and should work with any LLM that supports the ChatCompletion
    format, as the rest of our tools do.
    """

    # input and setup validation
    assert len(df) > 0, "Input dataframe should not be empty"
    assert 'code' in df.columns, "Input dataframe should have a 'code' column"
    assert chunk_size > 0 and isinstance(chunk_size, int), "`chunk_size` should be a positive integer"

    supported_formats = ['chat', 'fim']
    supported_langs = [e.value for e in Language]
    assert language.lower() in supported_langs, f"Invalid language. Valid choices are: {supported_langs}"

    # ensure correct encoding
    df['code'] = df['code'].map(lambda x: x.encode('utf8').decode('unicode_escape'))

    # set prompt templates
    system_prompt = "You are a powerful text to code model. Your job is to provide great code completions. As context, you are given code that is found immediately before and after the code you must generate.\n\nYou must output the code that should go in between the prefix and suffix.\n\n"
    if format == 'chat':
        templates = [f"### {c}:" for c in chat_sections]
    elif format == 'fim':
        templates = fim_tokens
    else:
        raise Exception(f"Invalid format. Please choose one of {supported_formats}")

    # split code into chunks
    code_splitter = RecursiveCharacterTextSplitter.from_language(
        language=getattr(Language, language.upper()),
        chunk_size=3 * chunk_size,  # each triplet element has `chunk_size`
        chunk_overlap=chunk_overlap,  # some overlap here is fine
    )
    chunk_docs = code_splitter.create_documents(list(df['code']))
    chunks = [c.page_content for c in chunk_docs]

    # split each chunk into a triplet, with no overlap
    triplet_splitter = RecursiveCharacterTextSplitter.from_language(
        language=getattr(Language, language.upper()),
        chunk_size=chunk_size,
        chunk_overlap=0,  # no overlap admitted, otherwise context may leak into answer
    )
    triplet_chunk_docs = triplet_splitter.create_documents(chunks)
    chunks = [c.page_content for c in triplet_chunk_docs]
    chunks = chunks[:len(chunks) - len(chunks) % 3]  # should be a multiple of 3

    # format chunks into prompts
    roles = []
    contents = []
    for idx in range(0, len(chunks), 3):
        pre, mid, suf = chunks[idx:idx + 3]
        interleaved = list(itertools.chain(*zip(templates, (pre, suf, mid))))
        user = "\n".join(interleaved[:-1])
        assistant = "\n".join(interleaved[-1:])
        roles.extend(['system', 'user', 'assistant'])
        contents.extend([system_prompt, user, assistant])

    # return formatted prompts in a dataframe to be processed by `ft_chat_formatter()`
    df = pd.DataFrame({'role': roles, 'content': contents})
    return df


def ft_cqa_formatter(
        df: pd.DataFrame,

        question_col='question',
        answer_col='answer',
        instruction_col='instruction',
        context_col='context',

        default_instruction='You are a helpful assistant.',
        default_context='',
) -> pd.DataFrame:

    # input and setup validation
    assert len(df) > 0, "Input dataframe should not be empty"
    assert {question_col, answer_col}.issubset(set(df.columns)), f"Input dataframe must have columns `{question_col}`, and `{answer_col}`"  # noqa

    if instruction_col not in df.columns:
        df[instruction_col] = default_instruction

    if context_col not in df.columns:
        df[context_col] = default_context

    # format data into chat-like prompts
    roles = []
    contents = []
    for i, row in df.iterrows():
        system = "\n".join([row[instruction_col], row[context_col]])
        user = row[question_col]
        assistant = row[answer_col]
        roles.extend(['system', 'user', 'assistant'])
        contents.extend([system, user, assistant])

    return pd.DataFrame({'role': roles, 'content': contents})

