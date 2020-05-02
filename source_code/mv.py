import networkx as nx
import time
import csv


# import random
def get_edge(dictionary, u, v):
    if u not in dictionary:
        return v
    else:
        return dictionary[u]


def add_node(dictionary, degree, node):
    if degree not in dictionary:
        dictionary[degree] = {}
        dictionary[degree]['root'] = [node, node]
        dictionary[degree][node] = ['root', 'root']
        dictionary[degree]['size'] = 1
    else:
        if node not in dictionary[degree]:
            last = dictionary[degree]['root'][0]
            dictionary[degree][last][1] = node
            dictionary[degree]['root'][0] = node
            dictionary[degree][node] = [last, 'root']
            dictionary[degree]['size'] += 1


def remove_node(dictionary, degree, node):
    prev = dictionary[degree][node][0]
    next = dictionary[degree][node][1]
    dictionary[degree][prev][1] = next
    dictionary[degree][next][0] = prev
    del dictionary[degree][node]
    dictionary[degree]['size'] -= 1


def mv_max_cardinality(H, list_matches, greedy_node_order=False, aug_phases=1):
    class petal:
        __slots__ = ['peaks', 'base']

        def __init__(self, base, peak):
            self.base = base
            self.peaks = peak

    def init_graph():
        global phaselist, G, total_nodes
        for node in G.nodes():
            if G.neighbors(node):
                node_pid[node] = -1
                node_petal[node] = None
                node_bud[node] = node
                node_level[node] = [0, float('inf')]
                node_min_level[node] = 0
                node_max_level[node] = float('inf')
                node_matched[node] = False
                node_pred[node] = []
                node_pred_tree[node] = node
                node_succ[node] = []
                node_color[node] = None
                node_visited[node] = None
                phaselist.append(node)
            else:
                G.remove_node(node)
        for edge in G.edges():
            if edge[0] == edge[1]:
                G.remove_edge(edge[0], edge[1])
            else:
                edge_scanned[edge] = -1
                edge_prop[edge] = None
                edge_matched[edge] = 0
                edge_augmented[edge] = 0
        for i in range(1, total_nodes + 2):
            bridge[i] = []

    def greedy_alg():
        global matched, G
        nodes = set([])
        node_list = []
        for u in list(G.nodes()):
            node_list.append((G.degree(u), u))
        sorted_list = sorted(node_list)
        for deg, u in sorted_list:
            if u not in nodes:
                for v in G.neighbors(u):
                    if v not in nodes:
                        nodes.add(u)
                        nodes.add(v)
                        node_matched[u] = True
                        node_matched[v] = True
                        edge_matched[tuple(sorted((u, v)))] = 1
                        edge_augmented[tuple(sorted((u, v)))] += 1
                        matched += 1
                        break

    def greedy_alg_2():
        global matched
        temp_g = nx.Graph()
        doubly_linked_degree = {}
        min_dld = float('inf')
        for e in G.edges():
            temp_g.add_edge(e[0], e[1])
            e1 = G.degree(e[0])
            e2 = G.degree(e[1])
            if e1 < min_dld:
                min_dld = e1
            if e2 < min_dld:
                min_dld = e2
            add_node(doubly_linked_degree, e1, e[0])
            add_node(doubly_linked_degree, e2, e[1])
        total_edges = nx.number_of_edges(temp_g)
        while total_edges > 0:
            if doubly_linked_degree[min_dld]['size'] == 0:
                while doubly_linked_degree[min_dld]['size'] == 0:
                    min_dld += 1
            u = doubly_linked_degree[min_dld]['root'][0]
            neighbors = temp_g.neighbors(u)
            if min_dld == 1:
                v = neighbors[0]
                v_degree = temp_g.degree(v)
                v_neighbors = temp_g.neighbors(v)
                remove_node(doubly_linked_degree, min_dld, u)
                remove_node(doubly_linked_degree, v_degree, v)
                for w in v_neighbors:
                    if w != u:
                        w_degree = temp_g.degree(w)
                        remove_node(doubly_linked_degree, w_degree, w)
                        total_edges -= 1
                        if w_degree - 1 != 0:
                            add_node(doubly_linked_degree, w_degree - 1, w)
                temp_g.remove_node(u)
                temp_g.remove_node(v)
                node_matched[u] = True
                node_matched[v] = True
                edge_matched[tuple(sorted((u, v)))] = 1
                total_edges -= 1
                edge_augmented[tuple(sorted((u, v)))] += 1
                matched += 1
            else:
                min_degree = float('inf')
                min_node = None
                for v in neighbors:
                    v_deg = temp_g.degree(v)
                    if v_deg < min_degree:
                        min_degree = v_deg
                        min_node = v
                # Collect and change nodes affected by the removal of u and v
                # First we handle nodes adjacent to u
                for v in neighbors:
                    total_edges -= 1
                    if v != min_node:
                        remove_node(doubly_linked_degree, temp_g.degree(v), v)
                min_node_neighbor = temp_g.neighbors(min_node)
                for v in min_node_neighbor:
                    total_edges -= 1
                    if v != u and v not in neighbors:
                        remove_node(doubly_linked_degree, temp_g.degree(v), v)
                # Remove Nodes from the temporary graph and add them to the matching
                remove_node(doubly_linked_degree, temp_g.degree(u), u)
                remove_node(doubly_linked_degree, temp_g.degree(min_node), min_node)
                temp_g.remove_node(u)
                temp_g.remove_node(min_node)
                node_matched[u] = True
                node_matched[min_node] = True
                edge_matched[tuple(sorted((u, min_node)))] = 1
                edge_augmented[tuple(sorted((u, min_node)))] += 1
                matched += 1
                # Add back the nodes to the doubly linked list
                for v in neighbors:
                    if v != min_node:
                        v_deg = temp_g.degree(v)
                        if v_deg > 0:
                            add_node(doubly_linked_degree, v_deg, v)
                            if min_dld > v_deg:
                                min_dld = v_deg
                for v in min_node_neighbor:
                    if v != u:
                        v_deg = temp_g.degree(v)
                        if v_deg > 0:
                            add_node(doubly_linked_degree, v_deg, v)
                            if min_dld > v_deg:
                                min_dld = v_deg

    # Starts a new phase after augmenting the graph
    def phase_reset():
        global phaselist, G, total_nodes
        phaselist = []
        for node in G.nodes():
            if node_matched[node]:
                node_min_level[node] = float('inf')
                node_max_level[node] = float('inf')
                node_level[node] = [float('inf'), float('inf')]
                node_pred_tree[node] = -1
            else:
                phaselist.append(node)
                node_min_level[node] = 0
                node_max_level[node] = float('inf')
                node_level[node] = [0, float('inf')]
                node_pred_tree[node] = node
            node_pred[node] = []
            node_succ[node] = []
            node_petal[node] = None
            node_bud[node] = node
            node_color[node] = None
            node_visited[node] = None
        for edge in G.edges():
            edge_prop[edge] = None
            edge_scanned[edge] = -1
        for i in range(1, int(2 * total_nodes + 1)):
            bridge[i] = []

    def MIN(phase):
        global phaselist, pid, G, total_nodes
        next_phase_list = []
        parity = phase % 2
        if not phaselist or phase > total_nodes:
            return True
        for u in phaselist:
            if node_level[u][parity] != phase and node_level[u][parity] < float('inf'):
                next_phase_list.append(u)
                continue
            if node_pid[u] == pid:
                continue
            for v in G.neighbors(u):
                e = tuple(sorted([u, v]))
                if edge_scanned[e] != pid and edge_matched[e] == parity and node_pid[v] != pid:
                    edge_scanned[e] = pid
                    if node_min_level[v] > phase:
                        node_min_level[v] = phase + 1
                        node_level[v][1 - parity] = phase + 1
                        next_phase_list.append(v)
                        node_pred[v].append(u)
                        # if node_pred_tree == -1:
                        node_pred_tree[v] = u
                        node_succ[u].append(v)
                        edge_prop[e] = True
                        # node_search_tree[v] += node_search_tree[u]
                    else:
                        tenacity = node_level[u][parity] + node_level[v][parity] + 1
                        # In the case where tenacity is defined and thus we know which level the bridge will be processed
                        if tenacity < float('inf'):
                            bridge[tenacity].append(e)
                        # The case where tenacity is not yet known (possibly due to the even/odd level of the blossom not yet labeled
                        else:
                            edge_prop[e] = False
        phaselist = next_phase_list  # Need to find a way to include max level nodes (This was solved)
        return False

    def MAX(phase):
        global G, matched, pid, aug_completed, aug_limit, total_bridges, aug_bridge, blossom_bridge, blossoms, total_nodes, prev_phase
        total_augmented = 0
        augmented = False
        # total_bridges = len(bridge[2*phase + 1]) + len(bridge_blossom[2*phase + 1])
        blossom_bridge = 0
        blossoms_max = 0
        deleted = 0
        # random.shuffle(bridge[2*phase + 1])
        bridges_in_phase = len(bridge[2 * phase + 1])
        for e in bridge[2 * phase + 1]:
            if node_pid[e[0]] == pid or node_pid[e[1]] == pid:
                deleted += 1
                continue
            left_support, right_support, bottleneck, pid_error = ddfs(e[0], e[1])
            # Bridge was augmented
            if bottleneck == None:
                if not pid_error:
                    aug_success = augment(left_support, right_support, e, phase)
                    if aug_success:
                        matched += 1
                        total_augmented += 1
                        augmented = True
                        if matched == total_nodes / 2:
                            return augmented
                    else:
                        print('aug failed')
                else:
                    'pid error?'
            else:
                if not pid_error:
                    formBlossom(left_support, right_support, bottleneck, e)
                    label_max(left_support, phase)
                    label_max(right_support, phase)
                    blossoms_max += 1
                else:
                    'pid error?'
        if augmented and not aug_completed == aug_limit:
            print('phase augmented: ' + str(phase))
            print('bridges: ' + str(bridges_in_phase))
            print('augmented: ' + str(total_augmented))
            print('blossomed: ' + str(blossoms_max))
            print('deleted: ' + str(deleted))
            print('')
            prev_phase = phase
            aug_completed += 1
            augmented = False
        return augmented

    # After finding support, we label the maxlevel of the node.
    #	Notice this is not used for when we augment since the max levels will be reset anyway
    def label_max(sup, phase):
        global phaselist, G

        next_phase_list = []
        for u in sup:
            l = 2 * phase + 1 - node_min_level[u]
            node_max_level[u] = l
            m_parity = l % 2
            node_level[u][m_parity] = l
            next_phase_list.append(u)
            if m_parity == 0:
                for v in G.neighbors(u):
                    e = tuple(sorted([u, v]))
                    # In the case were the tenacity of a bridge was not yet found
                    if edge_prop[e] == False:
                        bridge[l + node_level[v][0] + 1].append(e)
        phaselist += next_phase_list

    def ddfs(red, green):
        global pid
        pid_error = False
        # Set the starting point for each dfs
        stack_r, stack_g = [], []  # Stack saves previously traveled nodes
        r, g = getbud(red), getbud(green)  # Set the initial point for both dfs's
        r_pred, g_pred = [x for x in node_pred[r]], [x for x in
                                                     node_pred[g]]  # Copy predecessor array over for the current node
        r_support, g_support = [r], [g]  # the arrays holding the support of the current bridge
        # Following is used to save the data for dfs's for when they backtrack in the case a bottleneck is reached
        prev_r_support, prev_g_support = [r], [g]
        # Boolean variables are initiated
        no_aug_found = False if node_min_level[r] == 0 and node_min_level[g] == 0 else True
        collision = True if r == g else False
        # Returns nothing if there is no support for the petal
        if collision and not no_aug_found:
            return [], [], r, pid_error
        # Label is used to track if nodes have been visited in the current ddfs
        label = (red, green)
        node_visited[r], node_visited[g] = label, label
        # ddfs continues to run while an augmenting path still isn't found
        while no_aug_found:
            # Checks for when the two dfs's land on the same node
            if collision:
                # The the levels of the nodes are the same, we reverse the green dfs
                if node_min_level[r] == node_min_level[g]:
                    prev_g_support, min_bottle_g, bottle_g = [x for x in g_support], node_min_level[g], g
                    g, g_pred, reverse_check = reverse_dfs(g, g_pred, stack_g, g_support)
                    if reverse_check:
                        prev_r_support, bottle_r = [x for x in r_support], r
                        r, r_pred, reverse_check = reverse_dfs(r, r_pred, stack_r, r_support)
                elif node_min_level[r] > node_min_level[g]:
                    r, r_pred, reverse_check = reverse_dfs(r, r_pred, stack_r, r_support)
                elif node_min_level[r] < node_min_level[g]:
                    g, g_pred, reverse_check = reverse_dfs(g, g_pred, stack_g, g_support)
                if r == g:
                    prev_r_support.pop()
                    g_support.pop()
                    return prev_r_support, g_support, r, pid_error
                collision = False
            # Case where red dfs advances in search
            elif node_min_level[r] >= node_min_level[g]:
                # Advance the red dfs, will reverse if no nodes to travel to
                r, r_pred, collision = adv_dfs(r, r_pred, stack_r, r_support, label)
                # If stack is cleared and no nodes left to explore, bottleneck is found
                if not stack_r and not r_pred:
                    prev_r_support.pop()
                    g_support.pop()
                    return prev_r_support, g_support, g, pid_error
            # Case where green dfs advances in search
            else:
                # Advance the green dfs, will reverse if no nodes to travel to
                g, g_pred, collision = adv_dfs(g, g_pred, stack_g, g_support, label)
                # If stack is clearned and no nodes left to explore, reverse red dfs
                if not stack_g and not g_pred:
                    g_support = prev_g_support
                    prev_g_support, g, g_pred = [g], r, [x for x in r_pred]
                    prev_r_support, bottle_r = [x for x in r_support], r
                    r, r_pred, reverse_check = reverse_dfs(r, r_pred, stack_r, r_support)
                    if reverse_check:
                        prev_r_support.pop()
                        g_support.pop()
                        return prev_r_support, g_support, bottle_r, pid_error
            # Checks if node was removed in previous augmentation during current phase
            if node_pid[r] == pid or node_pid[g] == pid:
                pid_error = True
            # Checks if augmenting path has been found
            if node_min_level[r] == 0 and node_min_level[g] == 0 and r != g:
                no_aug_found = False

        return r_support, g_support, None, pid_error

    def adv_dfs(node, pred_arr, stack, support, label):
        reverse_check = False
        if pred_arr:
            next_node = getbud(pred_arr.pop())
            # Save the prev node with it's pred array to the stack
            stack.append([node, pred_arr])
            # Add next node to support
            support.append(next_node)
            pred_arr = [x for x in node_pred[next_node]]
            if node_visited[next_node] == label:
                return next_node, pred_arr, True
            node_visited[next_node] = label
        # If next node not found reverse path
        else:
            next_node, pred_arr, reverse_check = reverse_dfs(node, pred_arr, stack, support)
        return next_node, pred_arr, reverse_check

    def reverse_dfs(node, pred_arr, stack, support):
        failure = False
        if stack:
            prev_node = stack.pop()
            node = prev_node[0]
            pred_arr = prev_node[1]
            support.pop()
        else:
            failure = True
        return node, pred_arr, failure

    # Each node can only belong to one petal
    # The bud cannot be part of the petal
    # Each node in the petal points to the bud
    def formBlossom(left_sup, right_sup, bud, brg):
        global pid, blossoms
        blossoms += 1
        p = petal(bud, (brg[0], brg[1]))
        formPetal(left_sup, bud, p, 0)
        formPetal(right_sup, bud, p, 1)

    def formPetal(support, bud, petal, lr):
        for sup in support:
            node_bud[sup] = getbud(bud)
            node_petal[sup] = petal
            node_color[sup] = lr

    # Path compression
    def getbud(node):
        if node != node_bud[node]:
            node_bud[node] = getbud(node_bud[node])
        return node_bud[node]

    # Opens a petal to get the path from the node that is part of the petal to the bud
    def openPetal(node, target):
        path = []
        petal = node_petal[node]
        bud = petal.base
        if node_max_level[node] % 2 == 1:
            path = dfs_petal(node, bud, petal)
        else:
            r = petal.peaks[0]
            g = petal.peaks[1]
            if node_color[node] == 0:
                path_l = dfs_petal(r, node, petal)
                path_r = dfs_petal(g, bud, petal)
                if path_l and path_r:
                    path_l.reverse()
                    path = path_l + path_r
                else:
                    return False
            elif node_color[node] == 1:
                path_l = dfs_petal(r, bud, petal)
                path_r = dfs_petal(g, node, petal)
                if path_l and path_r:
                    path_r.reverse()
                    path = path_r + path_l
                else:
                    return False
        if bud == target:
            return path
        else:
            path.pop()
            petal_path = openPetal(bud, target)
            return path + petal_path

    def dfs_petal(hi, lo, petal):
        if hi == lo:
            return [hi]
        if node_petal[hi] != petal:
            new_target = node_petal[hi].base
            path = openPetal(hi, new_target)
            curr_node = new_target
        else:
            path = [hi]
            curr_node = hi
        while curr_node != lo:
            prev_node = curr_node
            pred_arr = [x for x in node_pred[curr_node]]
            new_petal = None
            next_petal_node = None
            wrong_petal_node = None
            for n in pred_arr:
                if node_petal[n] != None:
                    if n == lo:
                        curr_node = n
                        path.append(lo)
                        break
                    elif node_petal[n] == petal and node_color[curr_node] == node_color[n]:
                        next_petal_node = n
                    elif node_petal[n] == petal and node_color[curr_node] != node_color[n]:
                        wrong_petal_node = n
                    else:
                        new_petal = n
                else:
                    if n == petal.base:
                        curr_node = n
                        path.append(lo)
                        break
            if prev_node == curr_node:
                if next_petal_node != None:
                    curr_node = next_petal_node
                    path.append(curr_node)
                elif wrong_petal_node != None:
                    curr_node = n
                    petal1 = node_petal[n]
                    if petal1 != petal and petal1 != None:
                        petal_path = openPetal(curr_node, petal1.base)
                        path += petal_path
                        curr_node = path[-1]
                    else:
                        path.append(curr_node)
                elif new_petal == None:
                    return False
                else:
                    if edge_matched[tuple(sorted([curr_node, new_petal]))] == 0:
                        path_addition = openPetal(new_petal, node_petal[new_petal].base)
                        if not path_addition:
                            return False
                        path += path_addition
                        curr_node = node_petal[new_petal].base
                        while node_petal[curr_node] != petal and curr_node != lo:
                            print('petal digging')
                            path.pop()
                            if node_petal[curr_node]:
                                path += openPetal(curr_node, node_petal[curr_node].base)
                                curr_node = node_petal[curr_node].base
                            else:
                                print('bud failure')
                                return False
                    else:
                        path += dfs_petal(new_petal, node_petal[new_petal].base, node_petal[new_petal])
                        curr_node = node_petal[new_petal].base

        return path

    def augment(left_support, right_support, brg, phase):
        global pid
        l_path = getPath(left_support, brg[0])
        r_path = getPath(right_support, brg[1])
        if not l_path or not r_path:
            return False
        l_path.reverse()
        path = l_path + r_path
        if len(path) != phase * 2 + 2:
            print('augment length error')
            for i in range(len(path) - 1):
                print(edge_matched[tuple(sorted([path[i], path[i + 1]]))])
            for n in path:
                print(str(n) + ' ' + str(node_level[n]) + ' ' + str(node_pred[n]) + ' ' + str(node_succ[n]))
                for n1 in node_succ[n]:
                    print(str(n1) + ' ' + str(node_level[n1]))
            print('augment length error')
            return False
        prev_edge = 1
        saved_aug = {}
        for i in range(len(path) - 1):
            e = tuple(sorted([path[i], path[i + 1]]))
            if prev_edge == edge_matched[e]:
                print('augmentation path error at i = ' + str(i))
                return False
            prev_edge = edge_matched[e]
            saved_aug[e] = 1 - edge_matched[e]
        for e in saved_aug:
            edge_matched[e] = 1 - edge_matched[e]
            edge_augmented[e] += 1
        node_matched[l_path.pop(0)] = True
        node_matched[r_path.pop()] = True
        # Erase node based on phase id
        removal = []
        for i in path:
            node_pid[i] = pid
            removal.append(i)
        while removal:
            curr_node = removal.pop()
            successors = [x for x in node_succ[curr_node]]
            for n in successors:
                if node_pid[n] != pid:
                    node_pred[n].remove(curr_node)
                    node_succ[curr_node].remove(n)
                    if not node_pred[n]:
                        removal.append(n)
                        node_pid[n] = pid
        return True

    # Procedure to find path after discovering an augmenting path via ddfs
    def getPath(support, pk):
        path = []
        curr_node = pk
        pred_arr = [x for x in node_pred[curr_node]]
        # We follow the support given to get the path
        for node in support:
            # We do a search, following the trail given by the support
            while getbud(curr_node) != node:
                # If it is not the correct node we pop the next node in the pred array
                curr_node = pred_arr.pop()
            pred_arr = [x for x in node_pred[curr_node]]
            # If the node is not part of a petal, we add it to the path
            # Otherwise we need to open the petal
            if node_petal[curr_node] == None:
                path.append(curr_node)
            else:
                petal_path = openPetal(curr_node, getbud(curr_node))
                if not petal_path:
                    return False
                path += petal_path
                curr_node = getbud(curr_node)
                pred_arr = [x for x in node_pred[curr_node]]
        return path

    def search():
        global phase, prev_phase
        phase = 0
        prev_phase = 0
        aug = False
        done = False
        while not aug and not done:
            done = MIN(phase)
            aug = MAX(phase)
            if done and aug_completed == 1:
                print('end phase: ' + str(phase))
                return False
            phase += 1
        print('end phase: ' + str(phase))
        return True

    # Global dictionaries
    bridge = {}

    node_pid = {}
    node_visited = {}
    node_petal = {}
    node_bud = {}
    node_level = {}
    node_min_level = {}
    node_max_level = {}
    node_matched = {}
    node_pred = {}
    node_pred_tree = {}
    node_succ = {}
    node_color = {}

    edge_scanned = {}
    edge_matched = {}
    edge_prop = {}
    edge_augmented = {}

    global G, phaselist, matched, pid, blossoms, aug_completed, aug_limit, aug_bridge, blossom_bridge, total_bridges, total_nodes
    G = H.copy()
    phaselist = []

    blossoms = 0
    pid = 0
    matched = 0
    aug_completed = 1
    aug_limit = aug_phases
    aug_bridge = 0
    blossom_bridge = 0
    total_time = 0
    total_nodes = len(G.nodes())
    matched_arr = []

    run = True
    init_graph()
    start_time = time.time()
    if greedy_node_order:
        greedy_alg_2()
    else:
        greedy_alg()
    print('greedy algorithm time: ' + str(time.time() - start_time))
    print('matches: ' + str(matched))
    total_time += time.time() - start_time
    phase_reset()
    while run:
        start_time = time.time()
        prev_matched = matched
        blossoms = 0
        aug_bridge = 0
        blossom_bridge = 0
        aug_completed = 1
        total_bridges = 0
        print('\nphase: ' + str(pid))
        run = search()
        pid += 1
        alg_time = time.time() - start_time
        total_time += alg_time
        phase_time = time.time()
        phase_reset()
        print('phase time: ' + str(time.time() - phase_time))
        print('algorithm time: ' + str(alg_time))
        print('matches: ' + str(matched))
        print('blossoms: ' + str(blossoms))
        print('new matches: ' + str(matched - prev_matched))
        if matched - prev_matched != 0:
            matched_arr.append(matched - prev_matched)
        print('aug bridges: ' + str(aug_bridge))
        print('blossom bridges: ' + str(blossom_bridge))
        print('total bridges: ' + str(total_bridges))
        if matched == total_nodes / 2:
            break
    aug_dict = {}
    for e in edge_augmented:
        if edge_augmented[e] not in aug_dict:
            aug_dict[edge_augmented[e]] = 1
        else:
            aug_dict[edge_augmented[e]] += 1
    total_sum = 0
    percent_matched = []
    for i in reversed(matched_arr):
        total_sum += i
        percent_matched.insert(0, i / float(total_sum))
    print('percent matched: ' + str(percent_matched))
    print(aug_dict)
    if list_matches == 1:
        node_match_list = {}
        for e in edge_matched:
            if edge_matched[e] == 1:
                node_match_list[e[0]] = e[1]
                node_match_list[e[1]] = e[0]
        if len(node_match_list) != 2 * matched:
            print('error')
            print(matched)
        print('total time: ' + str(total_time))
        if percent_matched:
            average_percent_matched = sum(percent_matched) / float(len(percent_matched))
        else:
            average_percent_matched = 1
        return node_match_list, pid, average_percent_matched
    return matched
