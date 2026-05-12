import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.metrics import (
    accuracy_score,
    f1_score,
    recall_score,
    classification_report,
    confusion_matrix
)

def evaluate_model(model, X_val, y_val, model_name):
    y_pred = model.predict(X_val)
    
    acc     = accuracy_score(y_val, y_pred)
    f1_mac  = f1_score(y_val, y_pred, average='macro')
    f1_del  = f1_score(y_val, y_pred, labels=['delay'], average='macro')
    f1_ear  = f1_score(y_val, y_pred, labels=['early'], average='macro')
    f1_ont  = f1_score(y_val, y_pred, labels=['on_time'], average='macro')
    rec_del = recall_score(y_val, y_pred, labels=['delay'], average='macro')
    rec_ear = recall_score(y_val, y_pred, labels=['early'], average='macro')
    rec_ont = recall_score(y_val, y_pred, labels=['on_time'], average='macro')

    print(f"=== {model_name} ===")
    print(f"Accuracy:         {acc:.3f}")
    print(f"F1 Macro:         {f1_mac:.3f}")
    print(f"F1 Delay:         {f1_del:.3f}")
    print(f"F1 Early:         {f1_ear:.3f}")
    print(f"F1 On Time:       {f1_ont:.3f}")
    print(f"Recall Delay:     {rec_del:.3f}")
    print(f"Recall Early:     {rec_ear:.3f}")
    print(f"Recall On Time:   {rec_ont:.3f}")
    print(f"\n{classification_report(y_val, y_pred)}")

    cm = confusion_matrix(y_val, y_pred, labels=['delay', 'early', 'on_time'])
    plt.figure(figsize=(7, 5))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                xticklabels=['delay', 'early', 'on_time'],
                yticklabels=['delay', 'early', 'on_time'])
    plt.title(f'Confusion Matrix - {model_name}', fontsize=13)
    plt.xlabel('Predicted')
    plt.ylabel('Actual')
    plt.tight_layout()
    plt.show()

    return {
        'Model':          model_name,
        'Accuracy':       acc,
        'F1_Macro':       f1_mac,
        'F1_Delay':       f1_del,
        'F1_Early':       f1_ear,
        'F1_OnTime':      f1_ont,
        'Recall_Delay':   rec_del,
        'Recall_Early':   rec_ear,
        'Recall_OnTime':  rec_ont,
        'F1_Delay_Early': (f1_del + f1_ear) / 2
    }

results = []

def evaluate_model_pred(y_true, y_pred, model_name):
    acc     = accuracy_score(y_true, y_pred)
    f1_mac  = f1_score(y_true, y_pred, average='macro')
    f1_del  = f1_score(y_true, y_pred, labels=['delay'], average='macro')
    f1_ear  = f1_score(y_true, y_pred, labels=['early'], average='macro')
    f1_ont  = f1_score(y_true, y_pred, labels=['on_time'], average='macro')
    rec_del = recall_score(y_true, y_pred, labels=['delay'], average='macro')
    rec_ear = recall_score(y_true, y_pred, labels=['early'], average='macro')
    rec_ont = recall_score(y_true, y_pred, labels=['on_time'], average='macro')

    print(f"=== {model_name} ===")
    print(f"Accuracy:         {acc:.3f}")
    print(f"F1 Macro:         {f1_mac:.3f}")
    print(f"F1 Delay:         {f1_del:.3f}")
    print(f"F1 Early:         {f1_ear:.3f}")
    print(f"F1 On Time:       {f1_ont:.3f}")
    print(f"Recall Delay:     {rec_del:.3f}")
    print(f"Recall Early:     {rec_ear:.3f}")
    print(f"Recall On Time:   {rec_ont:.3f}")
    print(f"\n{classification_report(y_true, y_pred)}")

    cm = confusion_matrix(y_true, y_pred, labels=['delay', 'early', 'on_time'])
    plt.figure(figsize=(7, 5))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                xticklabels=['delay', 'early', 'on_time'],
                yticklabels=['delay', 'early', 'on_time'])
    plt.title(f'Confusion Matrix - {model_name}', fontsize=13)
    plt.xlabel('Predicted')
    plt.ylabel('Actual')
    plt.tight_layout()
    plt.show()

    return {
        'Model':          model_name,
        'Accuracy':       acc,
        'F1_Macro':       f1_mac,
        'F1_Delay':       f1_del,
        'F1_Early':       f1_ear,
        'F1_OnTime':      f1_ont,
        'Recall_Delay':   rec_del,
        'Recall_Early':   rec_ear,
        'Recall_OnTime':  rec_ont,
        'F1_Delay_Early': (f1_del + f1_ear) / 2
    }

def check_overfitting(model, X_train, y_train, X_val, y_val, model_name, le=None):
    
    if le is not None:
        y_train_pred = le.inverse_transform(model.predict(X_train))
        y_val_pred = le.inverse_transform(model.predict(X_val))
    else:
        y_train_pred = model.predict(X_train)
        y_val_pred = model.predict(X_val)
    
    metrics = {
        'Model': model_name,
        'Train Accuracy':    accuracy_score(y_train, y_train_pred),
        'Val Accuracy':      accuracy_score(y_val, y_val_pred),
        'Train F1 Macro':    f1_score(y_train, y_train_pred, average='macro'),
        'Val F1 Macro':      f1_score(y_val, y_val_pred, average='macro'),
        'Train F1 Early':    f1_score(y_train, y_train_pred, labels=['early'], average='macro'),
        'Val F1 Early':      f1_score(y_val, y_val_pred, labels=['early'], average='macro'),
    }
    
    metrics['Gap Accuracy'] = metrics['Train Accuracy'] - metrics['Val Accuracy']
    metrics['Gap F1 Macro'] = metrics['Train F1 Macro'] - metrics['Val F1 Macro']
    
    return metrics   


def check_overfitting_full(model, X_train, y_train, X_val, y_val, model_name, le=None):
    
    if le is not None:
        y_train_pred = le.inverse_transform(model.predict(X_train))
        y_val_pred = le.inverse_transform(model.predict(X_val))
    else:
        y_train_pred = model.predict(X_train)
        y_val_pred = model.predict(X_val)
    
    return {
        'Model': model_name,
        # Accuracy
        'Train Accuracy':    accuracy_score(y_train, y_train_pred),
        'Val Accuracy':      accuracy_score(y_val, y_val_pred),
        'Gap Accuracy':      accuracy_score(y_train, y_train_pred) - accuracy_score(y_val, y_val_pred),
        # F1 Macro
        'Train F1 Macro':    f1_score(y_train, y_train_pred, average='macro'),
        'Val F1 Macro':      f1_score(y_val, y_val_pred, average='macro'),
        'Gap F1 Macro':      f1_score(y_train, y_train_pred, average='macro') - f1_score(y_val, y_val_pred, average='macro'),
        # F1 Early
        'Train F1 Early':    f1_score(y_train, y_train_pred, labels=['early'], average='macro'),
        'Val F1 Early':      f1_score(y_val, y_val_pred, labels=['early'], average='macro'),
        'Gap F1 Early':      f1_score(y_train, y_train_pred, labels=['early'], average='macro') - f1_score(y_val, y_val_pred, labels=['early'], average='macro'),
        # F1 Delay
        'Train F1 Delay':    f1_score(y_train, y_train_pred, labels=['delay'], average='macro'),
        'Val F1 Delay':      f1_score(y_val, y_val_pred, labels=['delay'], average='macro'),
        'Gap F1 Delay':      f1_score(y_train, y_train_pred, labels=['delay'], average='macro') - f1_score(y_val, y_val_pred, labels=['delay'], average='macro'),
    }
