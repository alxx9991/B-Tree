#include "btreestore.h"

#include <pthread.h>

// Prints way keys are in the node
void print_node(struct btree_node *node) {
  printf("Number of keys: %d\n", node->num_keys);
  printf("Address of node: %p ", node);
  for (int i = 0; i < node->num_keys; i++) {
    printf("key: %d\n", node->key_value_pairs[i].key);
  }
  if (node->parent == NULL) {
    printf("\n");
    return;
  }

  printf("Address of children: %p\n\n", node->children);
  printf("\n");
}

// Pre-order traversal of tree, printing every node, starting from the root.
// Also checks that the parent pointers are correct
void print_btree(struct btree_node *root, struct btree_node *caller,
                 struct btree *btree) {
  if (btree->root != root) {
    if (root->parent != caller) {
      fprintf(stderr,
              "ERROR!!: node with address %p does not have the parent %p, but "
              "is instead %p\n",
              root, caller, root->parent);
    }
  }
  print_node(root);
  int num_of_children = root->num_keys + 1;
  if (root->is_leaf == 1) {
    return;
  }
  for (int i = 0; i < num_of_children; i++) {
    print_btree(&(root->children[i]), root, btree);
  }
}

// Check that parent pointers are correct for each child -> that each child has
// the correct parent. Return 0 for no errors, return 1 for error.
int check_parent_btree(struct btree_node *root, struct btree_node *caller,
                       struct btree *btree) {
  if (btree->root != root) {
    if (root->parent != caller) {
      fprintf(stderr,
              "ERROR!!: node with address %p does not have the parent %p, but "
              "is instead %p\n",
              root, caller, root->parent);
      return 1;
    }
  }
  int num_of_children = root->num_keys + 1;
  if (root->is_leaf == 1) {
    return 0;
  }
  int return_val = 0;
  for (int i = 0; i < num_of_children; i++) {
    if (check_parent_btree(&(root->children[i]), root, btree) != 0) {
      return_val = 1;
    };
  }
  return return_val;
}

// Pre-order traversal to free everything
void free_btree(struct btree_node *root) {
  int num_of_children = root->num_keys + 1;
  // If leaf, then free everything
  if (root->is_leaf == 1) {
    free(root->children);
    for (int i = 0; i < root->num_keys; i++) {
      free(root->key_value_pairs[i].encrypted_data);
    }
    free(root->key_value_pairs);
    return;
  }

  for (int i = 0; i < num_of_children; i++) {
    free_btree(
        &(root->children[i]));  // Once all the children are freed, free itself
  }
  free(root->children);

  // Free data inside keys
  for (int i = 0; i < root->num_keys; i++) {
    free(root->key_value_pairs[i].encrypted_data);
  }
  free(root->key_value_pairs);
}

// Pre-order traversal to count nodes
void count_nodes(struct btree_node *root, int *count) {
  (*count)++;
  int num_of_children = root->num_keys + 1;
  // If leaf, then count itself
  if (root->is_leaf == 1) {
    return;
  }
  for (int i = 0; i < num_of_children; i++) {
    count_nodes(&(root->children[i]),
                count);  // Once all the children are counted, count itself
  }
}

// Pre-order traversal to add stuff to export list
void export_recursive(struct btree_node *root, int *ptr,
                      struct node *node_list) {
  // Set current node to be the node struct we are currently filling out
  struct node *current_node = node_list + *ptr;

  // Copy in the num of keys
  if (root == NULL) {
    return;
  }
  current_node->num_keys = root->num_keys;

  // Loop over the keys and plug them in
  node_list[*ptr].keys = malloc(root->num_keys * sizeof(struct node));

  for (int i = 0; i < root->num_keys; i++) {
    node_list[*ptr].keys[i] = root->key_value_pairs[i].key;
  }

  *ptr += 1;
  int num_of_children = root->num_keys + 1;
  if (root->is_leaf == 1) {
    return;
  }
  for (int i = 0; i < num_of_children; i++) {
    export_recursive(root->children + i, ptr, node_list);
  }
}

// Search for a key-value pair, returns the address of the key data pair
struct key_data *search_key_data(struct btree_node *root, uint32_t key) {
  // If found in the keys list, then just return it
  for (int i = 0; i < root->num_keys; i++) {
    if (key == root->key_value_pairs[i].key) {
      return &(root->key_value_pairs[i]);
    }
  }
  // If in leaf node and not found, return NULL
  if (root->is_leaf == 1) {
    return NULL;
  }
  // If not leaf node and not found, then pick the correct child to search from
  for (int i = 0; i < root->num_keys; i++) {
    if (root->key_value_pairs[i].key > key) {
      return search_key_data(&(root->children[i]), key);
    }
  }
  // If the key is larger than every other child node, search the last child
  return search_key_data(&root->children[root->num_keys], key);
}

// Insert Helper functions
// Search for a node to put new key into. If the key already exists, returns
// NULL. Otherwise it returns the node which the key is meant to go in. This
// must be a leaf node.
struct btree_node *search_node_to_put_key(struct btree_node *root,
                                          uint32_t key) {
  // If found in the keys list, then return NULL
  for (int i = 0; i < root->num_keys; i++) {
    if (key == root->key_value_pairs[i].key) {
      return NULL;
    }
  }
  // If in leaf node and not found, return the node
  if (root->is_leaf == 1) {
    return root;
  }
  // If not leaf node and not found, then pick the correct child to search from
  for (int i = 0; i < root->num_keys; i++) {
    if (root->key_value_pairs[i].key > key) {
      return search_node_to_put_key(&(root->children[i]), key);
    }
  }
  // If the key is larger than every other child node, search the last child
  return search_node_to_put_key(&root->children[root->num_keys], key);
}

// Search for key within a node. If return is -1, it means the key already
// exists. Otherwise returns the index that the new key is supposed to be in.
uint32_t search_key_in_node(struct btree_node *node, uint32_t key) {
  uint32_t i;
  for (i = 0; i < node->num_keys; i++) {
    if (node->key_value_pairs[i].key == key) {
      return -1;
    }
    if (node->key_value_pairs[i].key > key) {
      return i;
    }
  }
  return node->num_keys;  // If the key is bigger than every other key, then
                          // just return the next index
}

// DELETE SEARCH
// Search for a node to delete key from. If the key does not exist, returns
// NULL. Otherwise it returns the node the key is in.
struct btree_node *search_node_to_delete_key(struct btree_node *root,
                                             uint32_t key) {
  // If found in the keys list, then return NULL
  for (int i = 0; i < root->num_keys; i++) {
    if (key == root->key_value_pairs[i].key) {
      return root;
    }
  }
  // If in leaf node and not found, return the node
  if (root->is_leaf == 1) {
    return NULL;
  }
  // If not leaf node and not found, then pick the correct child to search from
  for (int i = 0; i < root->num_keys; i++) {
    if (root->key_value_pairs[i].key > key) {
      return search_node_to_delete_key(&(root->children[i]), key);
    }
  }
  // If the key is larger than every other child node, search the last child
  return search_node_to_delete_key(&root->children[root->num_keys], key);
}

// Search for key within a node. If return is -1, it means the key doees not
// exists. Otherwise it returns the index that the new key is in.
int search_key_in_node_to_delete(struct btree_node *node, uint32_t key) {
  uint32_t i;
  // printf("Address of node: %p\n", node);
  for (i = 0; i < node->num_keys; i++) {
    if (node->key_value_pairs[i].key == key) {
      return i;
    }
  }
  return -1;  // If cant find key return -1
}

// Search for the node with the maximum key in the subtree rooted at root
struct btree_node *search_max_node(struct btree_node *root) {
  if (root->is_leaf) {
    // If the root has no children, then it is the max
    return root;
  }
  // Otherwise search the rightmost  child
  return search_max_node(root->children + root->num_keys);
}

// Split node function
void split_node(struct key_data *key_to_promote,
                struct btree_node *node_to_split, struct btree *btree) {
  uint32_t number_of_keys = node_to_split->num_keys;
  uint32_t median_index = floor((number_of_keys - 1) / 2);

  // Fill in key to promote
  copy_key_data(key_to_promote,
                &(node_to_split->key_value_pairs[median_index]));

  // Create space in the parent node, then use that space to initiate a new node
  uint32_t index_to_insert_child =
      search_key_in_node(node_to_split->parent, key_to_promote->key) + 1;
  if (index_to_insert_child == -1) {
    fprintf(stderr,
            "ERROR: In splitting, the key was already found in parent\n");
    exit(1);
  }

  // Move key data over by copying them
  memmove(&(node_to_split->parent->children[index_to_insert_child + 1]),
          &(node_to_split->parent->children[index_to_insert_child]),
          sizeof(struct btree_node) *
              (node_to_split->parent->num_keys + 1 - index_to_insert_child));

  struct btree_node *new_child =
      &(node_to_split->parent->children[index_to_insert_child]);
  new_child->parent = node_to_split->parent;
  new_child->is_leaf = node_to_split->is_leaf;
  new_child->key_value_pairs =
      malloc((btree->branching) * sizeof(struct key_data));
  new_child->children =
      malloc((btree->branching + 1) * sizeof(struct btree_node));
  new_child->num_keys = node_to_split->num_keys - median_index - 1;

  // Transfer some keys over to the new node, update the correct number of keys
  memmove(new_child->key_value_pairs,
          &(node_to_split->key_value_pairs[median_index + 1]),
          sizeof(struct key_data) * (new_child->num_keys));
  node_to_split->num_keys = median_index;

  // If the child is a leaf, we do not need to copy the old children over
  if (new_child->is_leaf == 1) {
    return;
  }

  // Copy children
  memmove(new_child->children, &(node_to_split->children[median_index + 1]),
          sizeof(struct btree_node) * (new_child->num_keys + 1));

  // Now loop over all of the children in new_child -> children, and set their
  // parent to the new child.

  // We also have to go and set all of their parents to the new cloned children,
  // if they are not a leaf node
  for (int k = 0; k < new_child->parent->num_keys + 2; k++) {
    for (int i = 0; i < new_child->parent->children[k].num_keys + 1; i++) {
      new_child->parent->children[k].children[i].parent =
          new_child->parent->children + k;
      if (new_child->parent->children[k].children->is_leaf == 1) {
        continue;
      }
      for (int j = 0;
           j < new_child->parent->children[k].children[i].num_keys + 1; j++) {
        new_child->parent->children[k].children[i].children[j].parent =
            &(new_child->parent->children[k].children[i]);
      }
    }
  }
}

// Recursive insert attempts to insert some key data information into a node. If
// the node is full, the node will be split and an insertion is attempted on the
// parent instead. If no parent, then make new root node, and then call split
// and insert again.
void recursive_insert(struct key_data *insertion_key_data,
                      struct btree_node *node, struct btree *btree) {
  // Calculate insertion index
  uint32_t insertion_idx = search_key_in_node(node, insertion_key_data->key);
  // Increment number of keys
  node->num_keys += 1;
  // Copy every key data struct from the insertion index onwards to one above
  memmove(&(node->key_value_pairs[insertion_idx + 1]),
          &(node->key_value_pairs[insertion_idx]),
          sizeof(struct key_data) * (node->num_keys - insertion_idx - 1));
  // Now copy the data for the key we are promoting to be used to insert into
  // the node above
  copy_key_data(&(node->key_value_pairs[insertion_idx]), insertion_key_data);
  // Check if we have overflowed. If not then exit.
  if (node->num_keys < btree->branching) {
    return;
  }

  // Check if num of keys exceeds the btree branching, then something has gone
  // wrong
  if (node->num_keys > btree->branching) {
    fprintf(stderr,
            "Something gone wrong, a node has two or more keys over the "
            "allowed limit. Max is one over the limit allowed\n");
    exit(1);
  }

  // Now check if the current node is the root node. If it is, then we need to
  // make a new root node.
  if (node == btree->root) {
    // Initialise new root and fill in the details
    struct btree_node *new_root =
        malloc((btree->branching + 1) * sizeof(struct btree_node));
    new_root->children =
        node;  // The root should have been initialised with some extra space
               // next to it which can be used to store some children
    new_root->key_value_pairs =
        malloc((btree->branching) * sizeof(struct key_data));
    new_root->is_leaf = 0;
    new_root->num_keys = 0;
    new_root->parent = NULL;

    // Make the old root have the new one as the parent
    btree->root->parent = new_root;

    // Now make the new btree root point to new root
    btree->root = new_root;
  }

  // Call split on the current node
  split_node(insertion_key_data, node, btree);

  // Now call insert again, except on the parent
  recursive_insert(insertion_key_data, node->parent, btree);
}

// Copies the data from one key data struct to another
void copy_key_data(struct key_data *destination, struct key_data *source) {
  destination->encrypted_data = source->encrypted_data;
  destination->encryption_key[0] = source->encryption_key[0];
  destination->encryption_key[1] = source->encryption_key[1];
  destination->encryption_key[2] = source->encryption_key[2];
  destination->encryption_key[3] = source->encryption_key[3];
  destination->key = source->key;
  destination->nonce = source->nonce;
  destination->size = source->size;
}

// Transfer key: Moves a key from one node to another, given the source index
// (which key u wanna move) and destination index (where u wanna put it).
int transfer_key(struct btree_node *destination, struct btree_node *source,
                 uint32_t destination_index, uint32_t source_index) {
  uint32_t keys_to_move_forward = destination->num_keys - destination_index;
  // We need to first make space inside destination
  memmove(destination->key_value_pairs + destination_index + 1,
          destination->key_value_pairs + destination_index,
          keys_to_move_forward * sizeof(struct key_data));
  // Then copy the key from the source to the destination, then update the
  // number of keys.
  struct key_data *source_key = source->key_value_pairs + source_index;
  // Find where to insert
  struct key_data *destination_key =
      destination->key_value_pairs + destination_index;
  // Copy
  memmove(destination_key, source_key, sizeof(struct key_data));
  uint32_t keys_to_move_backwards = source->num_keys - source_index - 1;
  // Shrink space inside the source
  memmove(source->key_value_pairs + source_index,
          source->key_value_pairs + source_index + 1,
          keys_to_move_backwards * sizeof(struct key_data));
  destination->num_keys += 1;
  source->num_keys -= 1;
}

// Recursive delete for btree1
int recursive_delete(struct btree_node *node_to_delete_from,
                     struct btree *btree) {
  // Get the index of the node in its children list
  uint32_t index_in_children_list =
      (uint32_t)((node_to_delete_from - node_to_delete_from->parent->children));
  int swap_res = key_swap(node_to_delete_from, btree, index_in_children_list);
  if (swap_res == 0) {
    // If key swap was successful, then we can return 0 and leave the recursion
    return 0;
  }
  node_to_delete_from = merge(node_to_delete_from, index_in_children_list);

  // After merging, check if the parent has got the minimum number
  uint32_t minimum_keys = (uint32_t)(ceil((double)btree->branching / 2)) - 1;

  if (node_to_delete_from->parent->num_keys >= minimum_keys) {
    return 0;
  }

  if (node_to_delete_from->parent == btree->root &&
      node_to_delete_from->parent->num_keys > 0) {
    return 0;
  }

  if (node_to_delete_from->parent->num_keys == 0 &&
      node_to_delete_from->parent == btree->root) {
    void *temp_old_root = btree->root;
    void *temp_child = btree->root->children;
    free(btree->root->key_value_pairs);
    free(btree->root);
    btree->root = temp_child;
    node_to_delete_from->parent = NULL;
    return 0;
  }

  return recursive_delete(node_to_delete_from->parent, btree);
}

// Merge, returns address of merged node
struct btree_node *merge(struct btree_node *node_to_delete_from,
                         uint32_t index_in_children_list) {
  struct btree_node *parent = node_to_delete_from->parent;

  // Pick someone to merge with
  if (index_in_children_list != 0) {
    // If child is not the left most, pick the left sibling
    struct btree_node *left_sibling =
        parent->children + (index_in_children_list - 1);
    uint32_t k_left_index = index_in_children_list - 1;

    // Save the number of children on the left sibling
    uint32_t num_children_to_merge = left_sibling->num_keys + 1;

    // Transfer key left from parent to one after the last key slot of left
    // sibling
    transfer_key(left_sibling, parent, left_sibling->num_keys, k_left_index);

    // Merge keys
    uint32_t num_keys_to_merge = left_sibling->num_keys;

    // Move the existing keys to the right to allow for space
    uint32_t existing_keys = node_to_delete_from->num_keys;
    memmove(node_to_delete_from->key_value_pairs + num_keys_to_merge,
            node_to_delete_from->key_value_pairs,
            existing_keys * sizeof(struct key_data));

    // Copy the left sibling keys onto the right sibling keys
    memmove(node_to_delete_from->key_value_pairs, left_sibling->key_value_pairs,
            sizeof(struct key_data) * num_keys_to_merge);

    // Update to the right number of keys
    node_to_delete_from->num_keys += num_keys_to_merge;

    // Transfer children
    // Make space
    memmove(node_to_delete_from->children + num_children_to_merge,
            node_to_delete_from->children,
            num_children_to_merge * sizeof(struct btree_node));

    // Copy over
    memmove(node_to_delete_from->children, left_sibling->children,
            sizeof(struct btree_node) * num_children_to_merge);

    free(left_sibling->key_value_pairs);
    free(left_sibling->children);

    // Remove the left sibling from its parent
    uint32_t children_to_shift = parent->num_keys - index_in_children_list + 2;
    memmove(parent->children + (index_in_children_list - 1),
            parent->children + (index_in_children_list),
            sizeof(struct btree_node) * children_to_shift);

    node_to_delete_from -= 1;  // Update the node to delete from pointer cos now
                               // its one to the left
    if (node_to_delete_from->is_leaf == 1) {
      return node_to_delete_from;
    }

    // Make the children have their parent pointer point to the merged node, and
    // ensure the children of the children point to the newly cloned children
    // Reshift the children's parents
    for (int k = index_in_children_list - 1;
         k < node_to_delete_from->parent->num_keys + 1; k++) {
      for (int i = 0; i < node_to_delete_from->parent->children[k].num_keys + 1;
           i++) {
        node_to_delete_from->parent->children[k].children[i].parent =
            &(node_to_delete_from->parent->children[k]);
        if (node_to_delete_from->parent->children[k].children[i].is_leaf == 1) {
          continue;  // If leaf, continue
        }

        for (int j = 0;
             j <
             node_to_delete_from->parent->children[k].children[i].num_keys + 1;
             j++) {
          node_to_delete_from->parent->children[k]
              .children[i]
              .children[j]
              .parent = node_to_delete_from->parent->children[k].children + i;
        }
      }
    }
    return node_to_delete_from;
  }

  // If child IS the left most, pick the right sibling - it will have one for
  // sure cos no one has only child node
  struct btree_node *right_sibling =
      parent->children + (index_in_children_list + 1);
  uint32_t k_right_index = index_in_children_list;

  // Save the number of children on the right sibling
  uint32_t num_children_to_merge = right_sibling->num_keys + 1;

  // Save the number of children originally
  uint32_t original_children = node_to_delete_from->num_keys + 1;

  // Transfer children
  // Copy over
  memmove(node_to_delete_from->children + node_to_delete_from->num_keys + 1,
          right_sibling->children,
          sizeof(struct btree_node) * num_children_to_merge);

  // Transfer key right from parent to one after the last key slot of the target
  // node
  transfer_key(node_to_delete_from, parent, node_to_delete_from->num_keys,
               k_right_index);
  // Merge keys
  uint32_t num_keys_to_merge = right_sibling->num_keys;

  // Copy the right sibling keys onto the target keys
  memmove(node_to_delete_from->key_value_pairs + node_to_delete_from->num_keys,
          right_sibling->key_value_pairs,
          sizeof(struct key_data) * num_keys_to_merge);

  // Update to the right number of keys
  node_to_delete_from->num_keys += num_keys_to_merge;
  free(right_sibling->key_value_pairs);
  free(right_sibling->children);

  // Remove the right sibling from its parent
  uint32_t children_to_shift = parent->num_keys - index_in_children_list + 1;
  memmove(parent->children + (index_in_children_list + 1),
          parent->children + (index_in_children_list + 2),
          sizeof(struct btree_node) * children_to_shift);

  if (node_to_delete_from->is_leaf == 1) {
    return node_to_delete_from;
  }

  // Make the children have their parent pointer point to the merged node, and
  // ensure the children of the children point to the newly cloned children
  for (int k = 0; k < node_to_delete_from->parent->num_keys + 1; k++) {
    for (int i = 0; i < node_to_delete_from->parent->children[k].num_keys + 1;
         i++) {
      node_to_delete_from->parent->children[k].children[i].parent =
          &(node_to_delete_from->parent->children[k]);
      if (node_to_delete_from->parent->children[k].children[i].is_leaf == 1) {
        continue;
      }
      for (int j = 0;
           j <
           node_to_delete_from->parent->children[k].children[i].num_keys + 1;
           j++) {
        node_to_delete_from->parent->children[k]
            .children[i]
            .children[j]
            .parent = node_to_delete_from->parent->children[k].children + i;
      }
    }
  }
  return node_to_delete_from;
}

// Key swap
// Steals keys from sibling, if it cannot then it will return 1. If success,
// return 0.
int key_swap(struct btree_node *node_to_delete_from, struct btree *btree,
             uint32_t index_in_children_list) {
  // We want to know the index position of the child in the parent. Subtract the
  // start of the parent array from its own address and then divide by size of
  // node.
  struct btree_node *parent = node_to_delete_from->parent;

  // Leaf swap
  if (node_to_delete_from->is_leaf == 1) {
    // Check to see if we can steal a sibling
    // First check if its a left most child or not
    if (index_in_children_list != 0) {
      // If its not, then check if the immediate sibling to the left has a spare
      // key
      struct btree_node *left_sibling =
          parent->children + (index_in_children_list - 1);
      if (left_sibling->num_keys > 1) {
        // Steal the key immediately to the left in the parent(its own index -
        // 1)
        transfer_key(node_to_delete_from, parent, 0,
                     index_in_children_list - 1);
        // Transfer a key from the left sibling to the parent
        transfer_key(parent, left_sibling, index_in_children_list - 1,
                     left_sibling->num_keys - 1);
        return 0;
      }
    }
    // If that didn't work, check to see if we are the right most sibling
    if (index_in_children_list != parent->num_keys) {
      // If its not, check the sibling to the right has a spare key
      struct btree_node *right_sibling =
          parent->children + index_in_children_list + 1;
      if (right_sibling->num_keys > 1) {
        // Steal the key immediately to the right in the parent(its own index),
        // set as the max key
        transfer_key(node_to_delete_from, parent, node_to_delete_from->num_keys,
                     index_in_children_list);

        // Transfer a key from the right sibling to the parent
        transfer_key(parent, right_sibling, index_in_children_list, 0);
        return 0;
      }
    }
    // If it cannot steal any keys, then return 1
    return 1;
  }

  // Internal swap -> different minimum number and also need to swap children
  // Check to see if we can steal a sibling
  // First check if its a left most child or not
  uint32_t minimum_keys = (uint32_t)(ceil((double)btree->branching / 2)) - 1;
  if (index_in_children_list != 0) {
    // If its not, then check if the immediate sibling to the left has a spare
    // key
    struct btree_node *left_sibling =
        parent->children + (index_in_children_list - 1);
    if (left_sibling->num_keys > minimum_keys) {
      // Steal the key immediately to the left in the parent(its own index - 1)
      transfer_key(node_to_delete_from, parent, 0, index_in_children_list - 1);

      // Transfer a key from the left sibling to the parent
      transfer_key(parent, left_sibling, index_in_children_list - 1,
                   left_sibling->num_keys - 1);

      // Steal a child from the left sibling
      // Make space in target node
      // At this point there will be an equal number of children and keys, so
      // num_keys = num_children
      memmove(node_to_delete_from->children + 1, node_to_delete_from->children,
              sizeof(struct btree_node) * node_to_delete_from->num_keys);

      // Copy it in and make it point to target node, and also fix the pointers
      // of its children as well
      memmove(node_to_delete_from->children,
              left_sibling->children + left_sibling->num_keys + 1,
              sizeof(struct btree_node));

      // Now we have to set it to point to the merged node
      node_to_delete_from->children->parent = node_to_delete_from;
      for (int i = 0; i < node_to_delete_from->children->num_keys; i++) {
        node_to_delete_from->children->children[i].parent =
            node_to_delete_from->children;
      }
      return 0;
    }
  }

  // If that didn't work, check to see if we are the right most sibling
  if (index_in_children_list != parent->num_keys) {
    // If its not, check the sibling to the right has a spare key
    struct btree_node *right_sibling =
        parent->children + index_in_children_list + 1;
    if (right_sibling->num_keys > minimum_keys) {
      // Steal the key immediately to the right in the parent(its own index),
      // set as the max key
      transfer_key(node_to_delete_from, parent, node_to_delete_from->num_keys,
                   index_in_children_list);

      // Transfer a key from the right sibling to the parent
      transfer_key(parent, right_sibling, index_in_children_list, 0);

      // Steal a child from the right sibling
      // Copy it in and make it point to target node, and also fix the pointers
      // of its children as well
      memmove(node_to_delete_from->children + node_to_delete_from->num_keys,
              right_sibling->children, sizeof(struct btree_node));

      // Now delete it from the right sibling
      memmove(right_sibling->children, right_sibling->children + 1,
              sizeof(struct btree_node) * (right_sibling->num_keys + 1));

      // Now have to fix the pointers inside the right sibling children
      for (int i = 0; i < right_sibling->num_keys + 1; i++) {
        if (right_sibling->children[i].is_leaf == 1) {
          break;
        }
        for (int j = 0; j < right_sibling->children[i].num_keys + 1; j++) {
          right_sibling->children[i].children[j].parent =
              right_sibling->children + i;
        }
      }

      // Now we have to set it to point to the merged node
      node_to_delete_from->children[node_to_delete_from->num_keys].parent =
          node_to_delete_from;
      for (int i = 0;
           i < node_to_delete_from->children[node_to_delete_from->num_keys]
                   .num_keys;
           i++) {
        node_to_delete_from->children[node_to_delete_from->num_keys]
            .children[i]
            .parent =
            node_to_delete_from->children + node_to_delete_from->num_keys;
      }

      return 0;
    }
  }
  // If it cannot steal any keys, then return 1
  return 1;
}

// Interface functions!
// init_store will initialise a btree object which has the root node, the
// branching factor, and the number of processors. Returns the address of btree
void *init_store(uint16_t branching, uint8_t n_processors) {
  struct btree *btree = malloc(sizeof(struct btree));
  pthread_mutex_init(&(btree->lock), NULL);
  pthread_mutex_lock(&(btree->lock));
  btree->branching = branching;
  btree->n_processors = n_processors;
  btree->root = NULL;
  pthread_mutex_unlock(&(btree->lock));
  return (void *)btree;
}

void close_store(void *helper) {
  struct btree *btree = (struct btree *)helper;
  pthread_mutex_lock(&(btree->lock));
  struct btree_node *root = btree->root;
  if (root != NULL) {
    free_btree(root);
    free(root);
  }
  free(btree);
  pthread_mutex_unlock(&(btree->lock));
  return;
}

int btree_insert(uint32_t key, void *plaintext, size_t count,
                 uint32_t encryption_key[4], uint64_t nonce, void *helper) {
  // Initialise local insertion key data struct
  struct key_data
      insertion_key_data;  // This is the key_data struct that is passed around
                           // the recursive function. It is the thing we are
                           // trying to insert into the next node. It starts off
                           // as the actual key we are inserting.
  insertion_key_data.key = key;

  // Figure out how many blocks of 64 we need, and how many bytes we need. Each
  // byte is 8 bits, so 8 bytes in 1 block. We need to round to the nearest 8
  // byte block.
  uint32_t num_of_blocks = (uint32_t)(ceil((double)count / 8));
  uint64_t num_of_bytes = (uint64_t)(8 * num_of_blocks);

  void *cipher_text = malloc(num_of_bytes);
  // Round up to nearest 64 bit chunk
  // First encrypt data , if the plaintext, count, encryption key etc are filled
  // in
  if (plaintext != NULL && encryption_key != NULL) {
    // Zero out last chunk, move to a block of 64
    void *plain_text = malloc(num_of_bytes);
    memmove(plain_text, plaintext, count);
    uint64_t size_to_zero_out =
        (uint64_t)num_of_bytes - count;  // subtract actual from rounded
    for (int i = 0; i < size_to_zero_out; i++) {
      ((unsigned char *)plain_text)[num_of_bytes - 1 - i] = 0;
    }

    encrypt_tea_ctr(plain_text, encryption_key, nonce, cipher_text,
                    num_of_blocks);  // Cipher_text shud now have encrypted data
    free(plain_text);

    // Now we can fill in the rest of the key data encryption stuff
    insertion_key_data.encryption_key[0] = encryption_key[0];
    insertion_key_data.encryption_key[1] = encryption_key[1];
    insertion_key_data.encryption_key[2] = encryption_key[2];
    insertion_key_data.encryption_key[3] = encryption_key[3];
    insertion_key_data.nonce = nonce;
    insertion_key_data.size = (uint32_t)count;
  }

  // Malloc even if no data
  insertion_key_data.encrypted_data = cipher_text;

  // Acquire lock before proceding to the tree
  struct btree *btree = (struct btree *)helper;
  pthread_mutex_lock(&(btree->lock));
  struct btree_node *root = btree->root;
  // If there is no root, then just make a new node and set the root pointer to
  // that.
  if (root == NULL) {
    struct btree_node *new_root = malloc(
        ((btree->branching) + 1) *
        sizeof(struct btree_node));  // Malloc extra space next to the root
                                     // node, in case it ever needs to have
                                     // siblings when it gets demoted
    new_root->children =
        malloc(((btree->branching) + 1) *
               sizeof(struct btree_node));  // malloc space for children, should
                                            // be one more than the max capacity
    new_root->is_leaf = 1;
    new_root->key_value_pairs =
        malloc((btree->branching) *
               sizeof(struct key_data));  // malloc space for keys, should be
                                          // one more than the max capacity
    new_root->num_keys = 1;
    // put the key data in the new root as the first key data set
    copy_key_data(new_root->key_value_pairs, &(insertion_key_data));
    new_root->parent = NULL;
    // Set new root of btree global
    btree->root = new_root;
    pthread_mutex_unlock(&(btree->lock));
    return 0;
  }

  // First search for the node to put the key in
  struct btree_node *target_node = search_node_to_put_key(root, key);
  if (target_node == NULL) {
    free(cipher_text);
    pthread_mutex_unlock(&(btree->lock));
    return 1;  // key already exists
  }

  // Begin recursive insertion
  recursive_insert(&insertion_key_data, target_node, btree);
  pthread_mutex_unlock(&(btree->lock));
  return 0;
}

int btree_retrieve(uint32_t key, struct info *found, void *helper) {
  struct btree *btree = (struct btree *)helper;
  pthread_mutex_lock(&(btree->lock));
  struct btree_node *root = btree->root;
  if (root == NULL) {
    pthread_mutex_unlock(&(btree->lock));
    return 1;
  }
  struct key_data *search_res = search_key_data(root, key);
  if (search_res == NULL) {
    pthread_mutex_unlock(&(btree->lock));
    return 1;
  }
  // fill in info * data struct
  if (search_res->key != key) {
    fprintf(stderr,
            "Error occured with searching, key found does not match key "
            "requested\n");
    exit(1);
  }
  found->data = search_res->encrypted_data;
  found->key[0] = search_res->encryption_key[0];
  found->key[1] = search_res->encryption_key[1];
  found->key[2] = search_res->encryption_key[2];
  found->key[3] = search_res->encryption_key[3];
  found->nonce = search_res->nonce;
  found->size = search_res->size;
  pthread_mutex_unlock(&(btree->lock));
  return 0;
}

int btree_decrypt(uint32_t key, void *output, void *helper) {
  // First we need to get the key value pair we need.
  struct btree *btree = (struct btree *)helper;
  pthread_mutex_lock(&(btree->lock));
  struct btree_node *root = btree->root;
  if (root == NULL) {
    pthread_mutex_unlock(&(btree->lock));
    return 1;
  }
  struct key_data *search_res = search_key_data(root, key);
  if (search_res == NULL) {
    pthread_mutex_unlock(&(btree->lock));
    return 1;
  }
  // Find out how many blocks there are
  uint32_t num_of_blocks = (uint32_t)(ceil((double)search_res->size / 8));
  void *plain = malloc(8 * num_of_blocks);

  // Then we perform decryption on it.
  decrypt_tea_ctr(search_res->encrypted_data, search_res->encryption_key,
                  search_res->nonce, plain, num_of_blocks);
  memmove(output, plain, search_res->size);
  free(plain);
  pthread_mutex_unlock(&(btree->lock));
  return 0;
}

int btree_delete(uint32_t key, void *helper) {
  struct btree *btree = (struct btree *)helper;
  pthread_mutex_lock(&(btree->lock));
  struct btree_node *root = btree->root;

  if (root == NULL) {
    pthread_mutex_unlock(&(btree->lock));
    return 1;
  }

  uint32_t minimum_keys = (uint32_t)ceil((double)btree->branching / 2) + 1;
  struct btree_node *node_to_delete_from = search_node_to_delete_key(root, key);

  // If no key found then exit with return code 1
  if (node_to_delete_from == NULL) {
    pthread_mutex_unlock(&(btree->lock));
    return 1;
  }

  uint32_t deletion_index =
      search_key_in_node_to_delete(node_to_delete_from, key);

  // If key is not in a leaf node, we need to swap it with the largest key in
  // its left child's subtree Swap and then exchange the node to delete
  if (node_to_delete_from->is_leaf == 0) {
    struct btree_node *max_subtree_node =
        search_max_node(node_to_delete_from->children + deletion_index);
    struct key_data *temp = malloc(sizeof(struct key_data));
    struct key_data *original_key =
        node_to_delete_from->key_value_pairs + deletion_index;
    struct key_data *max_key =
        max_subtree_node->key_value_pairs + max_subtree_node->num_keys - 1;

    // Perform swap
    memmove(temp, original_key, sizeof(struct key_data));
    memmove(original_key, max_key, sizeof(struct key_data));
    memmove(max_key, temp, sizeof(struct key_data));
    free(temp);
    // Set the leaf node as the one to delete
    node_to_delete_from = max_subtree_node;
    deletion_index = max_subtree_node->num_keys - 1;
  }

  // If key is in leaf node, proceed to base case
  if (node_to_delete_from->is_leaf != 1) {
    fprintf(stderr,
            "Error, internal node swapped for leaf node but the swapped one is "
            "not a leaf\n");
    exit(1);
  }

  if (deletion_index == -1) {
    fprintf(stderr,
            "Error occured - node to delete found does not have the index to "
            "delete in it\n");
    exit(1);
  }

  if (node_to_delete_from->key_value_pairs[deletion_index].key != key) {
    fprintf(stderr, "Error, key found to delete does not match requested\n");
    exit(1);
  }

  // Free the encrypted data inside the key to delete
  free(node_to_delete_from->key_value_pairs[deletion_index].encrypted_data);

  // Delete the key from the node
  memmove(node_to_delete_from->key_value_pairs + deletion_index,
          node_to_delete_from->key_value_pairs + deletion_index + 1,
          sizeof(struct key_data) *
              (node_to_delete_from->num_keys - deletion_index - 1));
  node_to_delete_from->num_keys -= 1;

  // If we still have more or equal to the minimum number of keys, we can leave
  if (node_to_delete_from->num_keys > 0) {
    pthread_mutex_unlock(&(btree->lock));
    return 0;
  }

  // If we just deleted the only key in the entire tree, then delete everything
  if (node_to_delete_from == btree->root &&
      node_to_delete_from->num_keys == 0 && node_to_delete_from->is_leaf == 1) {
    free(node_to_delete_from->key_value_pairs);
    free(node_to_delete_from->children);
    free(btree->root);
    btree->root = NULL;
    pthread_mutex_unlock(&(btree->lock));
    return 0;
  }

  // Begin recursive delete
  int res = recursive_delete(node_to_delete_from, btree);
  pthread_mutex_unlock(&(btree->lock));
  return res;
}

uint64_t btree_export(void *helper, struct node **list) {
  struct btree *btree = (struct btree *)helper;
  pthread_mutex_lock(&(btree->lock));
  struct btree_node *root = btree->root;
  if (root == NULL) {
    *list = NULL;
    pthread_mutex_unlock(&(btree->lock));
    return 0;
  }
  int number_of_nodes = 0;
  count_nodes(root, &number_of_nodes);
  struct node *export_list = malloc(number_of_nodes * sizeof(struct node));
  int ptr = 0;
  export_recursive(root, &ptr, export_list);
  *list = export_list;
  pthread_mutex_unlock(&(btree->lock));
  return number_of_nodes;
}

void encrypt_tea(uint32_t plain[2], uint32_t cipher[2], uint32_t key[4]) {
  uint32_t sum = 0;
  uint32_t delta = DELTA;
  cipher[0] = plain[0];
  cipher[1] = plain[1];

  // Just copy the sudo code
  uint32_t i = 0;
  while (i < 1024) {
    sum = (sum + delta) % (uint32_t)pow(2, 32);
    uint32_t tmp1 = ((cipher[1] << 4) + key[0]) % (uint32_t)pow(2, 32);
    uint32_t tmp2 = ((cipher[1] + sum) % (uint32_t)pow(2, 32));
    uint32_t tmp3 = ((cipher[1] >> 5) + key[1]) % (uint32_t)pow(2, 32);
    cipher[0] = (cipher[0] + (tmp1 ^ tmp2 ^ tmp3)) % (uint32_t)pow(2, 32);
    uint32_t tmp4 = ((cipher[0] << 4) + key[2]) % (uint32_t)pow(2, 32);
    uint32_t tmp5 = ((cipher[0] + sum)) % (uint32_t)pow(2, 32);
    uint32_t tmp6 = ((cipher[0] >> 5) + key[3]) % (uint32_t)pow(2, 32);
    cipher[1] = (cipher[1] + (tmp4 ^ tmp5 ^ tmp6)) % (uint32_t)pow(2, 32);
    i++;
  }
  return;
}

void decrypt_tea(uint32_t cipher[2], uint32_t plain[2], uint32_t key[4]) {
  uint32_t sum = SUM;
  uint32_t delta = DELTA;
  // Copied sudo code
  uint32_t i = 0;
  while (i < 1024) {
    uint32_t tmp4 = ((cipher[0] << 4) + key[2]) % (uint32_t)pow(2, 32);
    uint32_t tmp5 = (cipher[0] + sum) % (uint32_t)pow(2, 32);
    uint32_t tmp6 = ((cipher[0] >> 5) + key[3]) % (uint32_t)pow(2, 32);
    cipher[1] = (cipher[1] - (tmp4 ^ tmp5 ^ tmp6)) % (uint32_t)pow(2, 32);
    uint32_t tmp1 = ((cipher[1] << 4) + key[0]) % (uint32_t)pow(2, 32);
    uint32_t tmp2 = (cipher[1] + sum) % (uint32_t)pow(2, 32);
    uint32_t tmp3 = ((cipher[1] >> 5) + key[1]) % (uint32_t)pow(2, 32);
    cipher[0] = (cipher[0] - (tmp1 ^ tmp2 ^ tmp3)) % (uint32_t)pow(2, 32);
    sum = (sum - delta) % (uint32_t)pow(2, 32);
    i++;
  }
  plain[0] = cipher[0];
  plain[1] = cipher[1];
  return;
}

void encrypt_tea_ctr(uint64_t *plain, uint32_t key[4], uint64_t nonce,
                     uint64_t *cipher, uint32_t num_blocks) {
  uint64_t i = 0;
  while (i < num_blocks) {
    uint64_t tmp1 = i ^ nonce;
    uint64_t tmp2;
    encrypt_tea((uint32_t *)&tmp1, (uint32_t *)&tmp2, key);
    cipher[i] = plain[i] ^ tmp2;
    i++;
  }
  return;
}

void decrypt_tea_ctr(uint64_t *cipher, uint32_t key[4], uint64_t nonce,
                     uint64_t *plain, uint32_t num_blocks) {
  uint64_t i = 0;
  while (i < num_blocks) {
    uint64_t tmp1 = i ^ nonce;
    uint64_t tmp2;
    encrypt_tea((uint32_t *)&tmp1, (uint32_t *)&tmp2, key);
    plain[i] = cipher[i] ^ tmp2;
    i++;
  }
  return;
}
