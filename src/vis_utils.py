from bidi.algorithm import get_display
import arabic_reshaper

def fix_hebrew(text):
    try:
        reshaped = arabic_reshaper.reshape(str(text))
        return get_display(reshaped)
    except:
        return text

def trim_label(text, max_len=15):
    text = str(text)
    return text if len(text) <= max_len else '...' + text[-max_len:]