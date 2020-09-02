import math

def flatten(t):
    # (subtree, cummulative_pct)
    stack = []
    global_pct = {}
    stack.append((t, 1))
    
    while (True):
        if len(stack) == 0: break
        st = stack.pop()
        (parent, cpct) = st
        if len(parent) == 2:
            # This is a leaf
            (name, pct) = parent
            child_pct = cpct * pct
            global_pct[name] = round(child_pct, 10)            
            continue
            
        (name, pct, children) = parent
        new_cpct = pct * cpct        
        
        sum_pct = 0
        for node in children:
            if len(node) == 2:
                (_, child_pct) = node
            else:
                (_, child_pct, _) = node
            
            sum_pct += child_pct
            stack.append((node, new_cpct))        
        if sum_pct != 1:
            print("Children do not sum up to 100 pct!")
            return
    
    test_pct = 0
    for tp in global_pct.values():
        test_pct += tp
    test_pct = round(test_pct, 10)
    if test_pct != 1:
        print(global_pct)
        print(f"Pcts do not sum up to 100 pct! ({test_pct})")
        return
        
    return global_pct
            