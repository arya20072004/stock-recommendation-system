import json
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, confusion_matrix

def compute_metrics(predictions, field_pred, field_actual="actual_class"):
    if not predictions:
        return {"sample_size": 0}
        
    y_true = [p[field_actual] for p in predictions]
    y_pred = [p.get(field_pred, p["recommendation"]) for p in predictions]
    
    accuracy = accuracy_score(y_true, y_pred)
    # Define labels to ensure confusion matrix and prfs are consistently ordered
    labels = ["BUY", "HOLD", "SELL"]
    
    precision, recall, f1, support = precision_recall_fscore_support(
        y_true, y_pred, labels=labels, zero_division=0
    )
    
    macro_f1 = sum(f1) / len([s for s in support if s > 0]) if sum(support) > 0 else 0
    
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    cm_dict = {
        "BUY": {"BUY": int(cm[0][0]), "HOLD": int(cm[0][1]), "SELL": int(cm[0][2])},
        "HOLD": {"BUY": int(cm[1][0]), "HOLD": int(cm[1][1]), "SELL": int(cm[1][2])},
        "SELL": {"BUY": int(cm[2][0]), "HOLD": int(cm[2][1]), "SELL": int(cm[2][2])}
    }
    
    return {
        "sample_size": len(predictions),
        "accuracy": round(accuracy, 4),
        "precision": {l: round(p, 4) for l, p in zip(labels, precision)},
        "recall": {l: round(r, 4) for l, r in zip(labels, recall)},
        "f1": {l: round(f, 4) for l, f in zip(labels, f1)},
        "macro_f1": round(macro_f1, 4),
        "confusion_matrix": cm_dict
    }

def process_prediction_financials(predictions, field_pred):
    buy_returns = []
    sell_returns = []
    hold_returns = []
    
    buy_hits = 0
    sell_hits = 0
    
    for p in predictions:
        pred = p.get(field_pred, p["recommendation"])
        ret = p.get("actual_return")
        
        if ret is None:
            continue
            
        if pred == "BUY":
            buy_returns.append(ret) # actual_return
            if ret > 0: buy_hits += 1
        elif pred == "SELL":
            sell_returns.append(-ret) # -actual_return
            if ret < 0: sell_hits += 1
        elif pred == "HOLD":
            hold_returns.append(ret) # actual_return
            
    def agg(arr):
        return round(sum(arr)/len(arr), 4) if arr else None
        
    return {
        "BUY": {
            "average_directional_return": agg(buy_returns),
            "hit_rate": round(buy_hits / len(buy_returns), 4) if buy_returns else None,
            "sample_size": len(buy_returns)
        },
        "SELL": {
            "average_directional_return": agg(sell_returns),
            "hit_rate": round(sell_hits / len(sell_returns), 4) if sell_returns else None,
            "sample_size": len(sell_returns)
        },
        "HOLD": {
            "average_actual_return": agg(hold_returns),
            "sample_size": len(hold_returns)
        }
    }
