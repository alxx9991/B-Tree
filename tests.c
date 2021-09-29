#include <setjmp.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#include "btreestore.h"
#include "cmocka.h"

static int hard_coded_tree_setup(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 1);
  btree->root = malloc(sizeof(struct btree_node));
  // Name the root
  struct btree_node *root = btree->root;

  // Malloc stuff root
  root->key_value_pairs = malloc(3 * sizeof(struct key_data));
  root->children = malloc(4 * sizeof(struct btree_node));

  // Set values root
  root->is_leaf = 0;
  root->num_keys = 2;
  root->key_value_pairs[0].key = 7;
  root->key_value_pairs[1].key = 11;
  root->key_value_pairs[0].encrypted_data = malloc(10);
  root->key_value_pairs[1].encrypted_data = malloc(10);

  // Name the children
  struct btree_node *node_a = &(root->children[0]);
  struct btree_node *node_b = &(root->children[1]);
  struct btree_node *node_c = &(root->children[2]);

  // Malloc stuff A
  node_a->key_value_pairs = malloc(3 * sizeof(struct key_data));
  node_a->children = malloc(4 * sizeof(struct btree_node));

  // Set values A
  node_a->is_leaf = 0;
  node_a->num_keys = 1;
  node_a->key_value_pairs[0].key = 3;
  node_a->key_value_pairs[0].encrypted_data = malloc(10);

  // Name the children
  struct btree_node *node_d = &(node_a->children[0]);
  struct btree_node *node_e = &(node_a->children[1]);

  // Malloc stuff B
  node_b->key_value_pairs = malloc(3 * sizeof(struct key_data));
  node_b->children = malloc(4 * sizeof(struct btree_node));

  // Set values B
  node_b->is_leaf = 0;
  node_b->num_keys = 1;
  node_b->key_value_pairs[0].key = 9;
  node_b->key_value_pairs[0].encrypted_data = malloc(10);

  // Name the children
  struct btree_node *node_f = &(node_b->children[0]);
  struct btree_node *node_g = &(node_b->children[1]);

  // Malloc stuff C
  node_c->key_value_pairs = malloc(3 * sizeof(struct key_data));
  node_c->children = malloc(4 * sizeof(struct btree_node));

  // Set values C
  node_c->is_leaf = 0;
  node_c->num_keys = 2;
  node_c->key_value_pairs[0].key = 13;
  node_c->key_value_pairs[1].key = 15;
  node_c->key_value_pairs[0].encrypted_data = malloc(10);
  node_c->key_value_pairs[1].encrypted_data = malloc(10);

  // Name the children
  struct btree_node *node_h = &(node_c->children[0]);
  struct btree_node *node_i = &(node_c->children[1]);
  struct btree_node *node_j = &(node_c->children[2]);

  // Malloc stuff D
  node_d->key_value_pairs = malloc(3 * sizeof(struct key_data));
  node_d->children = malloc(4 * sizeof(struct btree_node));

  // Set values D
  node_d->is_leaf = 1;
  node_d->num_keys = 1;
  node_d->key_value_pairs[0].key = 1;
  node_d->key_value_pairs[0].encrypted_data = malloc(10);

  // Malloc stuff E
  node_e->key_value_pairs = malloc(3 * sizeof(struct key_data));
  node_e->children = malloc(4 * sizeof(struct btree_node));

  // Set values E
  node_e->is_leaf = 1;
  node_e->num_keys = 2;
  node_e->key_value_pairs[0].key = 4;
  node_e->key_value_pairs[1].key = 6;
  node_e->key_value_pairs[0].encrypted_data = malloc(10);
  node_e->key_value_pairs[1].encrypted_data = malloc(10);

  // Malloc stuff F
  node_f->key_value_pairs = malloc(3 * sizeof(struct key_data));
  node_f->children = malloc(4 * sizeof(struct btree_node));

  // Set values F
  node_f->is_leaf = 1;
  node_f->num_keys = 1;
  node_f->key_value_pairs[0].key = 8;
  node_f->key_value_pairs[0].encrypted_data = malloc(10);

  // Malloc stuff G
  node_g->key_value_pairs = malloc(3 * sizeof(struct key_data));
  node_g->children = malloc(4 * sizeof(struct btree_node));

  // Set values G
  node_g->is_leaf = 1;
  node_g->num_keys = 1;
  node_g->key_value_pairs[0].key = 10;
  node_g->key_value_pairs[0].encrypted_data = malloc(10);

  // Malloc stuff H
  node_h->key_value_pairs = malloc(3 * sizeof(struct key_data));
  node_h->children = malloc(4 * sizeof(struct btree_node));

  // Set values H
  node_h->is_leaf = 1;
  node_h->num_keys = 1;
  node_h->key_value_pairs[0].key = 12;
  node_h->key_value_pairs[0].encrypted_data = malloc(10);

  // Malloc stuff I
  node_i->key_value_pairs = malloc(3 * sizeof(struct key_data));
  node_i->children = malloc(4 * sizeof(struct btree_node));

  // Set values I
  node_i->is_leaf = 1;
  node_i->num_keys = 1;
  node_i->key_value_pairs[0].key = 14;
  node_i->key_value_pairs[0].encrypted_data = malloc(10);

  // Malloc stuff J
  node_j->key_value_pairs = malloc(3 * sizeof(struct key_data));
  node_j->children = malloc(4 * sizeof(struct btree_node));

  // Set values J
  node_j->is_leaf = 1;
  node_j->num_keys = 1;
  node_j->key_value_pairs[0].key = 16;
  node_j->key_value_pairs[0].encrypted_data = malloc(10);

  *state = btree;

  return 0;
}
static void init_store_test(void **state) {
  void *helper = init_store(4, 4);
  assert_true(((struct btree *)helper)->branching == 4);
  assert_true(((struct btree *)helper)->n_processors == 4);
  close_store(helper);
}

static void tea_ctr_test(void **state) {
  // initialise data
  uint64_t plain[4] = {100, 200, 10, 20};
  uint32_t key[4] = {20, 30, 98, 12};
  uint64_t cipher[4] = {};
  uint64_t nonce = 999;
  uint64_t new_plain[4] = {};

  // Encrypt and ensure that the cipher and plain are not the same
  encrypt_tea_ctr(plain, key, nonce, cipher, 4);
  assert_false(cipher[0] == plain[0]);
  assert_false(cipher[1] == plain[1]);
  assert_false(cipher[2] == plain[2]);
  assert_false(cipher[3] == plain[3]);

  // Decrypt, ensure new plain is equal to the original
  decrypt_tea_ctr(cipher, key, nonce, new_plain, 4);
  assert_true(new_plain[0] == plain[0]);
  assert_true(new_plain[1] == plain[1]);
  assert_true(new_plain[2] == plain[2]);
  assert_true(new_plain[3] == plain[3]);
}

static void hard_coded_tree_test_print_all(void **state) {
  struct btree *btree = (struct btree *)*state;
  struct btree_node *root = btree->root;
  // print_btree(root);
  close_store(btree);
}

static void hard_coded_tree_export_test(void **state) {
  struct btree *btree = (struct btree *)*state;
  struct btree_node *root = btree->root;
  struct node **node_list = malloc(sizeof(struct node *));
  btree_export(btree, node_list);
  struct node *test_list = *node_list;

  assert_true(test_list[0].num_keys == 2);
  assert_true(test_list[1].num_keys == 1);
  assert_true(test_list[2].num_keys == 1);
  assert_true(test_list[3].num_keys == 2);
  assert_true(test_list[4].num_keys == 1);
  assert_true(test_list[5].num_keys == 1);
  assert_true(test_list[6].num_keys == 1);
  assert_true(test_list[7].num_keys == 2);
  assert_true(test_list[8].num_keys == 1);
  assert_true(test_list[9].num_keys == 1);
  assert_true(test_list[10].num_keys == 1);

  assert_true(test_list[0].keys[0] == 7);
  assert_true(test_list[0].keys[1] == 11);
  assert_true(test_list[1].keys[0] == 3);
  assert_true(test_list[2].keys[0] == 1);
  assert_true(test_list[3].keys[0] == 4);
  assert_true(test_list[3].keys[1] == 6);
  assert_true(test_list[4].keys[0] == 9);
  assert_true(test_list[5].keys[0] == 8);
  assert_true(test_list[6].keys[0] == 10);
  assert_true(test_list[7].keys[0] == 13);
  assert_true(test_list[7].keys[1] == 15);
  assert_true(test_list[8].keys[0] == 12);
  assert_true(test_list[9].keys[0] == 14);
  assert_true(test_list[10].keys[0] == 16);

  // Free the key lists
  for (int i = 0; i < 11; i++) {
    free(test_list[i].keys);
  }
  // Free the entire list
  free(*node_list);
  free(node_list);
  close_store(btree);
}

static void search_test_hard_code(void **state) {
  struct btree *btree = (struct btree *)*state;
  struct btree_node *root = btree->root;

  struct key_data *target =
      &(root->children[2]
            .children[1]
            .key_value_pairs[0]);  // should be key value pair with 14 in it
  struct key_data *result = search_key_data(root, 14);
  assert_true(target == result);

  target = &(root->children[0].children[1].key_value_pairs[1]);  // search 6
  result = search_key_data(root, 6);
  assert_true(target == result);

  target = &(root->children[1].key_value_pairs[0]);  // search 9
  result = search_key_data(root, 9);
  assert_true(target == result);

  result = search_key_data(root, 99);  // Search something that doesn't exist
  assert_true(NULL == result);

  close_store(btree);
}

static void insert_empty_tree_test(void **state) {
  struct btree *btree = (struct btree *)init_store(2, 2);
  assert_true(btree->root == NULL);
  btree_insert(3, NULL, 0, NULL, 0, btree);

  assert_true(btree->root != NULL);
  assert_true(btree->root->key_value_pairs[0].key == 3);
  assert_true(btree->root->is_leaf == 1);
  assert_true(btree->root->num_keys == 1);
  assert_true(btree->root->parent == NULL);
  close_store(btree);
}

static void insert_to_root_no_split(void **state) {
  struct btree *btree = (struct btree *)init_store(4, 2);
  btree_insert(3, NULL, 0, NULL, 0, btree);
  btree_insert(4, NULL, 0, NULL, 0, btree);
  btree_insert(1, NULL, 0, NULL, 0, btree);
  assert_true(btree->root->num_keys == 3);
  assert_true(btree->root->key_value_pairs[0].key == 1);
  assert_true(btree->root->key_value_pairs[1].key == 3);
  assert_true(btree->root->key_value_pairs[2].key == 4);
  close_store(btree);
}

static void insert_to_root_split(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);

  btree_insert(3, NULL, 0, NULL, 0, btree);
  btree_insert(4, NULL, 0, NULL, 0, btree);

  struct btree_node *old_root = btree->root;

  btree_insert(1, NULL, 0, NULL, 0, btree);

  // print_btree(btree->root);

  struct btree_node *new_root = btree->root;
  assert_true(old_root != new_root);
  assert_true(new_root->is_leaf == 0);
  assert_true(new_root->children == old_root);
  assert_true(old_root->parent == new_root);

  struct btree_node *old_root_sibling = &(new_root->children[1]);
  assert_true(old_root->num_keys == 1);
  assert_true(old_root_sibling->num_keys == 1);
  assert_true(old_root->is_leaf == 1);
  assert_true(old_root_sibling->is_leaf == 1);
  assert_true(old_root->key_value_pairs[0].key == 1);
  assert_true(old_root_sibling->key_value_pairs[0].key == 4);

  assert_true(new_root->key_value_pairs[0].key == 3);
  assert_true((new_root->children) + 1 == old_root_sibling);
  assert_true(old_root_sibling->parent == new_root);
  assert_true(old_root->parent == new_root);

  close_store(btree);
}

static void spam_insert_test(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  btree_insert(3, NULL, 0, NULL, 0, btree);
  btree_insert(4, NULL, 0, NULL, 0, btree);
  btree_insert(1, NULL, 0, NULL, 0, btree);
  btree_insert(33, NULL, 0, NULL, 0, btree);
  btree_insert(42, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(983, NULL, 0, NULL, 0, btree);
  btree_insert(434, NULL, 0, NULL, 0, btree);
  btree_insert(451, NULL, 0, NULL, 0, btree);
  btree_insert(331, NULL, 0, NULL, 0, btree);
  btree_insert(41, NULL, 0, NULL, 0, btree);
  btree_insert(461, NULL, 0, NULL, 0, btree);
  btree_insert(309, NULL, 0, NULL, 0, btree);
  btree_insert(422, NULL, 0, NULL, 0, btree);
  btree_insert(100, NULL, 0, NULL, 0, btree);
  btree_insert(1400, NULL, 0, NULL, 0, btree);
  btree_insert(1300, NULL, 0, NULL, 0, btree);
  btree_insert(1070, NULL, 0, NULL, 0, btree);
  btree_insert(1021, NULL, 0, NULL, 0, btree);
  btree_insert(1530, NULL, 0, NULL, 0, btree);
  btree_insert(1350, NULL, 0, NULL, 0, btree);
  btree_insert(9990, NULL, 0, NULL, 0, btree);
  btree_insert(200, NULL, 0, NULL, 0, btree);
  btree_insert(18, NULL, 0, NULL, 0, btree);
  btree_insert(2, NULL, 0, NULL, 0, btree);
  btree_insert(24, NULL, 0, NULL, 0, btree);
  btree_insert(8, NULL, 0, NULL, 0, btree);
  btree_insert(297, NULL, 0, NULL, 0, btree);
  btree_insert(2111, NULL, 0, NULL, 0, btree);
  btree_insert(4572, NULL, 0, NULL, 0, btree);
  btree_insert(23345, NULL, 0, NULL, 0, btree);
  btree_insert(254, NULL, 0, NULL, 0, btree);
  btree_insert(2245, NULL, 0, NULL, 0, btree);
  btree_insert(26573, NULL, 0, NULL, 0, btree);
  // print_btree(btree -> root);
  close_store(btree);
}

static void search_node_to_put_key_test(void **state) {
  struct btree *btree = (struct btree *)*state;
  struct btree_node *root = btree->root;

  struct btree_node *target = &(root->children[0].children[1]);  //
  struct btree_node *result =
      search_node_to_put_key(root, 5);  // where to put 5??
  assert_true(target == result);
  uint32_t idx = 1;
  uint32_t index_result = search_key_in_node(result, 5);
  assert_true(idx == index_result);

  target = &(root->children[2].children[2]);
  result = search_node_to_put_key(root, 17);  // where to put 17?
  assert_true(idx == index_result);
  idx = 1;
  index_result = search_key_in_node(result, 17);
  assert_true(idx == index_result);

  result = search_node_to_put_key(
      root, 13);  // Search tree for 13 which is already in the tree
  assert_true(result == NULL);

  idx = -1;
  index_result = search_key_in_node(
      root, 11);  // search root node for 11 which is already in there
  assert_true(index_result == idx);

  close_store(btree);
}

static void encrypt_insert_decrypt_single(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  char *sample_text = "Hi i stoopid";
  size_t count = 13;
  uint32_t encryption_k[4] = {1, 2, 3, 4};
  uint64_t nonce = 2341134;

  btree_insert(3, sample_text, count, encryption_k, nonce, btree);
  assert_true(btree->root->key_value_pairs[0].key = 3);
  assert_true(btree->root->key_value_pairs[0].size = 13);
  assert_true(btree->root->key_value_pairs[0].nonce = 2341134);
  assert_true(btree->root->key_value_pairs[0].encryption_key[1] = 2);
  btree_insert(5, sample_text, count, encryption_k, nonce, btree);

  char *sample_text_serach = "Hi i stoopiddddd";
  size_t count_search = 17;
  uint32_t encryption_k_search[4] = {1, 2, 3, 3};
  uint64_t nonce_search = 2341131;

  btree_insert(9, sample_text_serach, count_search, encryption_k_search,
               nonce_search, btree);
  btree_insert(10, sample_text, count, encryption_k, nonce, btree);
  btree_insert(31, sample_text, count, encryption_k, nonce, btree);
  btree_insert(2, sample_text, count, encryption_k, nonce, btree);
  btree_insert(7, sample_text, count, encryption_k, nonce, btree);
  btree_insert(8, sample_text, count, encryption_k, nonce, btree);
  struct info found;
  int search_res = btree_retrieve(9, &found, btree);
  assert_true(search_res == 0);
  assert_true(found.key[0] == 1);
  assert_true(found.key[1] == 2);
  assert_true(found.key[2] == 3);
  assert_true(found.key[3] == 3);
  assert_true(found.size == 17);
  assert_true(found.nonce == 2341131);

  void *plain = malloc(24);
  int decrypt_res = btree_decrypt(9, plain, btree);
  unsigned char *final_text = malloc(17);
  memmove(final_text, plain, 17);
  assert_true(strcmp(final_text, sample_text_serach) == 0);

  free(plain);
  free(final_text);

  close_store(btree);
}

static void delete_leaf_test_no_merge(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  btree_insert(3, NULL, 0, NULL, 0, btree);
  btree_insert(1, NULL, 0, NULL, 0, btree);
  btree_insert(4, NULL, 0, NULL, 0, btree);
  btree_insert(5, NULL, 0, NULL, 0, btree);
  // print_btree(btree->root);
  int res = btree_delete(5, btree);
  assert_true(res == 0);
  assert_true(search_key_data(btree->root, 5) == NULL);
  close_store(btree);
}

static void delete_internal_no_merge(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  btree_insert(50, NULL, 0, NULL, 0, btree);
  btree_insert(45, NULL, 0, NULL, 0, btree);
  btree_insert(40, NULL, 0, NULL, 0, btree);
  btree_insert(20, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(15, NULL, 0, NULL, 0, btree);
  btree_insert(25, NULL, 0, NULL, 0, btree);
  btree_insert(16, NULL, 0, NULL, 0, btree);
  btree_insert(17, NULL, 0, NULL, 0, btree);
  // print_btree(btree->root);
  int res = btree_delete(20, btree);
  assert_true(res == 0);
  assert_true(search_key_data(btree->root, 20) == NULL);
  assert_true(btree->root->key_value_pairs[0].key == 17);
  close_store(btree);
}

static void delete_leaf_steal_left_sibling_once(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  btree_insert(50, NULL, 0, NULL, 0, btree);
  btree_insert(45, NULL, 0, NULL, 0, btree);
  btree_insert(40, NULL, 0, NULL, 0, btree);
  btree_insert(20, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(15, NULL, 0, NULL, 0, btree);
  btree_insert(25, NULL, 0, NULL, 0, btree);
  btree_insert(16, NULL, 0, NULL, 0, btree);
  btree_insert(12, NULL, 0, NULL, 0, btree);
  int res = btree_delete(20, btree);
  // print_btree(btree->root);
  assert_true(res == 0);
  assert_true(search_key_data(btree->root, 20) == NULL);
  assert_true(btree->root->key_value_pairs[0].key == 16);
  assert_true(btree->root->children[0].children[0].key_value_pairs[0].key ==
              10);
  close_store(btree);
}

static void delete_leaf_steal_right_sibling_once(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  btree_insert(50, NULL, 0, NULL, 0, btree);
  btree_insert(45, NULL, 0, NULL, 0, btree);
  btree_insert(40, NULL, 0, NULL, 0, btree);
  btree_insert(20, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(15, NULL, 0, NULL, 0, btree);
  btree_insert(25, NULL, 0, NULL, 0, btree);
  btree_insert(16, NULL, 0, NULL, 0, btree);
  btree_insert(17, NULL, 0, NULL, 0, btree);
  int res = btree_delete(10, btree);
  // print_btree(btree->root);
  assert_true(res == 0);
  assert_true(search_key_data(btree->root, 10) == NULL);
  assert_true(btree->root->key_value_pairs[0].key == 20);
  assert_true(btree->root->children[0].num_keys == 1);
  assert_true(btree->root->children[0].key_value_pairs[0].key == 16);
  assert_true(btree->root->children[0].children[1].num_keys == 1);
  assert_true(btree->root->children[0].children[1].key_value_pairs[0].key ==
              17);
  assert_true(btree->root->children[0].children[0].num_keys == 1);
  assert_true(btree->root->children[0].children[0].key_value_pairs[0].key ==
              15);
  close_store(btree);
}

static void merge_left_once_test(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  btree_insert(50, NULL, 0, NULL, 0, btree);
  btree_insert(45, NULL, 0, NULL, 0, btree);
  btree_insert(40, NULL, 0, NULL, 0, btree);
  btree_insert(20, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(15, NULL, 0, NULL, 0, btree);
  btree_insert(25, NULL, 0, NULL, 0, btree);
  btree_insert(16, NULL, 0, NULL, 0, btree);
  btree_insert(17, NULL, 0, NULL, 0, btree);
  btree_insert(18, NULL, 0, NULL, 0, btree);
  // print_btree(btree->root);
  int res = btree_delete(18, btree);
  assert_true(res == 0);
  assert_true(search_key_data(btree->root, 18) == NULL);
  assert_true(btree->root->key_value_pairs[0].key == 20);
  assert_true(btree->root->children[0].num_keys == 1);
  assert_true(btree->root->children[0].key_value_pairs[0].key == 15);
  assert_true(btree->root->children[0].children[0].num_keys == 1);
  assert_true(btree->root->children[0].children[0].key_value_pairs[0].key ==
              10);
  // print_btree(btree->root);
  assert_true(btree->root->children[0].children[1].num_keys == 2);
  assert_true(btree->root->children[0].children[1].key_value_pairs[0].key ==
              16);
  assert_true(btree->root->children[0].children[1].key_value_pairs[1].key ==
              17);
  close_store(btree);
}

static void merge_right_once_test(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  btree_insert(50, NULL, 0, NULL, 0, btree);
  btree_insert(45, NULL, 0, NULL, 0, btree);
  btree_insert(40, NULL, 0, NULL, 0, btree);
  btree_insert(20, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(15, NULL, 0, NULL, 0, btree);
  btree_insert(25, NULL, 0, NULL, 0, btree);
  btree_insert(16, NULL, 0, NULL, 0, btree);
  btree_insert(17, NULL, 0, NULL, 0, btree);
  btree_insert(18, NULL, 0, NULL, 0, btree);
  // print_btree(btree->root);
  int res = btree_delete(10, btree);
  // print_btree(btree->root);
  assert_true(res == 0);
  assert_true(search_key_data(btree->root, 10) == NULL);
  assert_true(btree->root->key_value_pairs[0].key == 20);
  assert_true(btree->root->children[0].num_keys == 1);
  assert_true(btree->root->children[0].key_value_pairs[0].key == 17);
  assert_true(btree->root->children[0].children[0].num_keys == 2);
  assert_true(btree->root->children[0].children[0].key_value_pairs[0].key ==
              15);
  assert_true(btree->root->children[0].children[0].key_value_pairs[1].key ==
              16);
  assert_true(btree->root->children[0].children[1].num_keys == 1);
  assert_true(btree->root->children[0].children[1].key_value_pairs[0].key ==
              18);
  close_store(btree);
}

static void search_max_test(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  btree_insert(3, NULL, 0, NULL, 0, btree);
  btree_insert(4, NULL, 0, NULL, 0, btree);
  btree_insert(1, NULL, 0, NULL, 0, btree);
  btree_insert(33, NULL, 0, NULL, 0, btree);
  btree_insert(42, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(983, NULL, 0, NULL, 0, btree);
  btree_insert(434, NULL, 0, NULL, 0, btree);
  btree_insert(451, NULL, 0, NULL, 0, btree);
  btree_insert(331, NULL, 0, NULL, 0, btree);
  struct btree_node *max_node = search_max_node(btree->root);
  assert_true(max_node->key_value_pairs[max_node->num_keys - 1].key == 983);
  btree_insert(41, NULL, 0, NULL, 0, btree);
  btree_insert(461, NULL, 0, NULL, 0, btree);
  btree_insert(309, NULL, 0, NULL, 0, btree);
  btree_insert(422, NULL, 0, NULL, 0, btree);
  btree_insert(100, NULL, 0, NULL, 0, btree);
  btree_insert(1400, NULL, 0, NULL, 0, btree);
  btree_insert(1300, NULL, 0, NULL, 0, btree);
  btree_insert(1070, NULL, 0, NULL, 0, btree);
  btree_insert(1021, NULL, 0, NULL, 0, btree);
  btree_insert(1530, NULL, 0, NULL, 0, btree);
  btree_insert(1350, NULL, 0, NULL, 0, btree);
  btree_insert(9990, NULL, 0, NULL, 0, btree);
  btree_insert(200, NULL, 0, NULL, 0, btree);
  btree_insert(18, NULL, 0, NULL, 0, btree);
  btree_insert(2, NULL, 0, NULL, 0, btree);
  btree_insert(24, NULL, 0, NULL, 0, btree);
  max_node = search_max_node(btree->root);
  assert_true(max_node->key_value_pairs[max_node->num_keys - 1].key == 9990);
  btree_insert(8, NULL, 0, NULL, 0, btree);
  btree_insert(297, NULL, 0, NULL, 0, btree);
  btree_insert(2111, NULL, 0, NULL, 0, btree);
  btree_insert(4572, NULL, 0, NULL, 0, btree);
  btree_insert(23345, NULL, 0, NULL, 0, btree);
  btree_insert(254, NULL, 0, NULL, 0, btree);
  btree_insert(2245, NULL, 0, NULL, 0, btree);
  btree_insert(26573, NULL, 0, NULL, 0, btree);
  max_node = search_max_node(btree->root);
  assert_true(max_node->key_value_pairs[max_node->num_keys - 1].key == 26573);
  close_store(btree);
}

static void test_non_recursive_delete_multiple(void **state) {
  struct btree *btree = (struct btree *)init_store(4, 2);
  btree_insert(3, NULL, 0, NULL, 0, btree);
  btree_insert(4, NULL, 0, NULL, 0, btree);
  btree_insert(1, NULL, 0, NULL, 0, btree);
  btree_insert(33, NULL, 0, NULL, 0, btree);
  btree_insert(42, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(983, NULL, 0, NULL, 0, btree);
  btree_insert(434, NULL, 0, NULL, 0, btree);
  btree_insert(451, NULL, 0, NULL, 0, btree);
  btree_insert(331, NULL, 0, NULL, 0, btree);
  btree_insert(41, NULL, 0, NULL, 0, btree);
  btree_insert(461, NULL, 0, NULL, 0, btree);
  btree_insert(309, NULL, 0, NULL, 0, btree);
  btree_insert(2, NULL, 0, NULL, 0, btree);
  btree_insert(5, NULL, 0, NULL, 0, btree);
  btree_insert(6, NULL, 0, NULL, 0, btree);
  btree_insert(7, NULL, 0, NULL, 0, btree);
  btree_insert(8, NULL, 0, NULL, 0, btree);
  btree_insert(9, NULL, 0, NULL, 0, btree);
  btree_insert(1000, NULL, 0, NULL, 0, btree);
  btree_delete(10, btree);
  btree_delete(4, btree);
  btree_delete(9, btree);
  btree_delete(451, btree);
  btree_delete(434, btree);
  btree_delete(33, btree);
  btree_delete(41, btree);

  assert_true(search_key_data(btree->root, 10) == NULL);
  assert_true(btree->root->key_value_pairs[0].key == 8);
  assert_true(btree->root->children[0].num_keys == 2);
  assert_true(btree->root->children[0].key_value_pairs[0].key == 2);
  assert_true(btree->root->children[0].children[0].num_keys == 1);
  assert_true(btree->root->children[0].children[0].key_value_pairs[0].key == 1);
  assert_true(btree->root->children[0].children[1].num_keys == 1);
  assert_true(btree->root->children[0].children[1].key_value_pairs[0].key == 3);
  // print_btree(btree->root);
  assert_true(btree->root->children[1].num_keys == 2);
  assert_true(btree->root->children[1].key_value_pairs[1].key == 983);
  assert_true(btree->root->children[1].children[0].num_keys == 2);
  assert_true(btree->root->children[1].children[0].key_value_pairs[1].key ==
              309);
  assert_true(btree->root->children[1].children[1].num_keys == 1);
  assert_true(btree->root->children[1].children[1].key_value_pairs[0].key ==
              461);

  close_store(btree);
}

static void simple_recursive_delete_left(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  btree_insert(3, NULL, 0, NULL, 0, btree);
  btree_insert(4, NULL, 0, NULL, 0, btree);
  btree_insert(1, NULL, 0, NULL, 0, btree);
  btree_insert(33, NULL, 0, NULL, 0, btree);
  btree_insert(42, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(983, NULL, 0, NULL, 0, btree);
  btree_insert(434, NULL, 0, NULL, 0, btree);
  btree_insert(451, NULL, 0, NULL, 0, btree);
  btree_insert(331, NULL, 0, NULL, 0, btree);
  btree_insert(41, NULL, 0, NULL, 0, btree);
  btree_insert(461, NULL, 0, NULL, 0, btree);
  btree_insert(309, NULL, 0, NULL, 0, btree);
  btree_insert(350, NULL, 0, NULL, 0, btree);
  // Simple merge to the right
  // Internal left key swap
  int res = btree_delete(451, btree);
  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  assert_true(res == 0);
  assert_true(btree->root->children[1].num_keys == 1);
  assert_true(btree->root->children[1].key_value_pairs[0].key == 42);
  assert_true(btree->root->children[2].key_value_pairs[0].key == 434);
  assert_true(btree->root->children[2].children[1].num_keys == 2);
  assert_true(btree->root->children[2].children[1].key_value_pairs[0].key ==
              461);
  // Simple merge to the left
  // Internal merge to the left
  btree_delete(331, btree);
  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  assert_true(res == 0);
  assert_true(btree->root->num_keys == 1);
  assert_true(btree->root->children[0].num_keys == 2);
  assert_true(btree->root->children[0].key_value_pairs[0].key == 3);
  assert_true(btree->root->children[1].key_value_pairs[0].key == 434);
  assert_true(btree->root->children[0].children[2].num_keys == 2);
  assert_true(btree->root->children[0].children[1].key_value_pairs[0].key == 4);
  // print_btree(btree->root, NULL, btree);
  close_store(btree);
}

static void simple_recursive_delete_right(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  btree_insert(3, NULL, 0, NULL, 0, btree);
  btree_insert(4, NULL, 0, NULL, 0, btree);
  btree_insert(1, NULL, 0, NULL, 0, btree);
  btree_insert(33, NULL, 0, NULL, 0, btree);
  btree_insert(42, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(983, NULL, 0, NULL, 0, btree);
  btree_insert(434, NULL, 0, NULL, 0, btree);
  btree_insert(451, NULL, 0, NULL, 0, btree);
  btree_insert(331, NULL, 0, NULL, 0, btree);
  btree_insert(41, NULL, 0, NULL, 0, btree);
  btree_insert(461, NULL, 0, NULL, 0, btree);
  btree_insert(309, NULL, 0, NULL, 0, btree);
  btree_insert(350, NULL, 0, NULL, 0, btree);
  btree_delete(4, btree);

  // Simple merge to the left
  // Internal right key swap
  int res = btree_delete(33, btree);
  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  assert_true(res == 0);
  assert_true(btree->root->children[0].num_keys == 1);
  assert_true(btree->root->children[0].key_value_pairs[0].key == 10);
  assert_true(btree->root->children[0].children[0].num_keys == 2);
  assert_true(btree->root->children[0].children[1].key_value_pairs[0].key ==
              41);
  // printf("After delete 33\n");
  // print_btree(btree->root, NULL, btree);
  btree_delete(1, btree);
  // Simple merge to the right
  // Internal merge to the right
  // print_btree(btree->root, NULL, btree);
  res = btree_delete(3, btree);
  // printf("Printing AFTER delete 3\n");
  // print_btree(btree->root, NULL, btree);
  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  assert_true(res == 0);
  assert_true(btree->root->num_keys == 1);
  assert_true(btree->root->children[0].num_keys == 2);
  assert_true(btree->root->children[0].key_value_pairs[0].key == 42);
  assert_true(btree->root->children[0].key_value_pairs[1].key == 331);
  assert_true(btree->root->children[0].children[2].num_keys == 1);
  assert_true(btree->root->children[0].children[1].key_value_pairs[0].key ==
              309);
  // print_btree(btree->root, NULL, btree);
  btree_delete(41, btree);
  btree_delete(309, btree);
  res = btree_delete(451, btree);
  // print_btree(btree->root, NULL, btree);
  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);

  close_store(btree);
}
static void delete_only_node(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  btree_insert(3, NULL, 0, NULL, 0, btree);
  btree_delete(3, btree);
  assert_true(btree->root == NULL);
  close_store(btree);
}
static void massive_deletion_recursion_to_empty(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  
  btree_insert(3, NULL, 0, NULL, 0, btree);
  btree_insert(4, NULL, 0, NULL, 0, btree);
  btree_insert(1, NULL, 0, NULL, 0, btree);
  btree_insert(33, NULL, 0, NULL, 0, btree);
  btree_insert(42, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(983, NULL, 0, NULL, 0, btree);
  btree_insert(434, NULL, 0, NULL, 0, btree);
  btree_insert(451, NULL, 0, NULL, 0, btree);
  btree_insert(331, NULL, 0, NULL, 0, btree);
  btree_insert(41, NULL, 0, NULL, 0, btree);
  btree_insert(461, NULL, 0, NULL, 0, btree);
  btree_insert(309, NULL, 0, NULL, 0, btree);
  btree_insert(422, NULL, 0, NULL, 0, btree);
  btree_insert(100, NULL, 0, NULL, 0, btree);
  btree_insert(1400, NULL, 0, NULL, 0, btree);
  btree_insert(1300, NULL, 0, NULL, 0, btree);
  btree_insert(1070, NULL, 0, NULL, 0, btree);
  btree_insert(1021, NULL, 0, NULL, 0, btree);
  btree_insert(1530, NULL, 0, NULL, 0, btree);
  btree_insert(1350, NULL, 0, NULL, 0, btree);
  btree_insert(9990, NULL, 0, NULL, 0, btree);
  btree_insert(200, NULL, 0, NULL, 0, btree);
  btree_insert(18, NULL, 0, NULL, 0, btree);
  btree_insert(2, NULL, 0, NULL, 0, btree);
  btree_insert(24, NULL, 0, NULL, 0, btree);
  btree_insert(8, NULL, 0, NULL, 0, btree);
  btree_insert(297, NULL, 0, NULL, 0, btree);
  btree_insert(2111, NULL, 0, NULL, 0, btree);
  btree_insert(4572, NULL, 0, NULL, 0, btree);
  btree_insert(23345, NULL, 0, NULL, 0, btree);
  btree_insert(254, NULL, 0, NULL, 0, btree);
  btree_insert(2245, NULL, 0, NULL, 0, btree);
  btree_insert(26573, NULL, 0, NULL, 0, btree);

  btree_delete(42, btree);
  btree_delete(2, btree);
  btree_delete(1350, btree);
  btree_delete(200, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(100, btree);
  btree_delete(18, btree);
  btree_delete(434, btree);
  btree_delete(10, btree);
  btree_delete(1021, btree);
  btree_delete(26573, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(297, btree);
  btree_delete(4572, btree);
  btree_delete(451, btree);
  btree_delete(1300, btree);
  btree_delete(9990, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(8, btree);
  btree_delete(2111, btree);
  btree_delete(422, btree);
  btree_delete(1400, btree);
  btree_delete(1070, btree);
  btree_delete(2245, btree);
  btree_delete(23345, btree);
  btree_delete(4, btree);
  btree_delete(3, btree);
  btree_delete(254, btree);
  btree_delete(309, btree);
  btree_delete(1, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(33, btree);
  btree_delete(331, btree);
  btree_delete(983, btree);
  btree_delete(1530, btree);
  btree_delete(461, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(24, btree);
  btree_delete(41, btree);
  assert_true(btree->root == NULL);
  close_store(btree);
}
static void massive_deletion_recursion_to_empty_bf_4(void **state) {
  struct btree *btree = (struct btree *)init_store(4, 2);
  btree_insert(3, NULL, 0, NULL, 0, btree);
  btree_insert(4, NULL, 0, NULL, 0, btree);
  btree_insert(1, NULL, 0, NULL, 0, btree);
  btree_insert(33, NULL, 0, NULL, 0, btree);
  btree_insert(42, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(983, NULL, 0, NULL, 0, btree);
  btree_insert(434, NULL, 0, NULL, 0, btree);
  btree_insert(451, NULL, 0, NULL, 0, btree);
  btree_insert(331, NULL, 0, NULL, 0, btree);
  btree_insert(41, NULL, 0, NULL, 0, btree);
  btree_insert(461, NULL, 0, NULL, 0, btree);
  btree_insert(309, NULL, 0, NULL, 0, btree);
  btree_insert(422, NULL, 0, NULL, 0, btree);
  btree_insert(100, NULL, 0, NULL, 0, btree);
  btree_insert(1400, NULL, 0, NULL, 0, btree);
  btree_insert(1300, NULL, 0, NULL, 0, btree);
  btree_insert(1070, NULL, 0, NULL, 0, btree);
  btree_insert(1021, NULL, 0, NULL, 0, btree);
  btree_insert(1530, NULL, 0, NULL, 0, btree);
  btree_insert(1350, NULL, 0, NULL, 0, btree);
  btree_insert(9990, NULL, 0, NULL, 0, btree);
  btree_insert(200, NULL, 0, NULL, 0, btree);
  btree_insert(18, NULL, 0, NULL, 0, btree);
  btree_insert(2, NULL, 0, NULL, 0, btree);
  btree_insert(24, NULL, 0, NULL, 0, btree);
  btree_insert(8, NULL, 0, NULL, 0, btree);
  btree_insert(297, NULL, 0, NULL, 0, btree);
  btree_insert(2111, NULL, 0, NULL, 0, btree);
  btree_insert(4572, NULL, 0, NULL, 0, btree);
  btree_insert(23345, NULL, 0, NULL, 0, btree);
  btree_insert(254, NULL, 0, NULL, 0, btree);
  btree_insert(2245, NULL, 0, NULL, 0, btree);
  btree_insert(26573, NULL, 0, NULL, 0, btree);

  btree_delete(42, btree);
  btree_delete(2, btree);
  btree_delete(1350, btree);
  btree_delete(200, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(100, btree);
  btree_delete(18, btree);
  btree_delete(434, btree);
  btree_delete(10, btree);
  btree_delete(1021, btree);
  btree_delete(26573, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(297, btree);
  btree_delete(4572, btree);
  btree_delete(451, btree);
  btree_delete(1300, btree);
  btree_delete(9990, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(8, btree);
  btree_delete(2111, btree);
  btree_delete(422, btree);
  btree_delete(1400, btree);
  btree_delete(1070, btree);
  btree_delete(2245, btree);
  btree_delete(23345, btree);
  btree_delete(4, btree);
  btree_delete(3, btree);
  btree_delete(254, btree);
  btree_delete(309, btree);
  btree_delete(1, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(33, btree);
  btree_delete(331, btree);
  btree_delete(983, btree);
  btree_delete(1530, btree);
  btree_delete(461, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(24, btree);
  btree_delete(41, btree);
  assert_true(btree->root == NULL);
  close_store(btree);
}

static void massive_deletion_recursion_to_empty_bf_7(void **state) {
  struct btree *btree = (struct btree *)init_store(7, 2);
  btree_insert(3, NULL, 0, NULL, 0, btree);
  btree_insert(4, NULL, 0, NULL, 0, btree);
  btree_insert(1, NULL, 0, NULL, 0, btree);
  btree_insert(33, NULL, 0, NULL, 0, btree);
  btree_insert(42, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(983, NULL, 0, NULL, 0, btree);
  btree_insert(434, NULL, 0, NULL, 0, btree);
  btree_insert(451, NULL, 0, NULL, 0, btree);
  btree_insert(331, NULL, 0, NULL, 0, btree);
  btree_insert(41, NULL, 0, NULL, 0, btree);
  btree_insert(461, NULL, 0, NULL, 0, btree);
  btree_insert(309, NULL, 0, NULL, 0, btree);
  btree_insert(422, NULL, 0, NULL, 0, btree);
  btree_insert(100, NULL, 0, NULL, 0, btree);
  btree_insert(1400, NULL, 0, NULL, 0, btree);
  btree_insert(1300, NULL, 0, NULL, 0, btree);
  btree_insert(1070, NULL, 0, NULL, 0, btree);
  btree_insert(1021, NULL, 0, NULL, 0, btree);
  btree_insert(1530, NULL, 0, NULL, 0, btree);
  btree_insert(1350, NULL, 0, NULL, 0, btree);
  btree_insert(9990, NULL, 0, NULL, 0, btree);
  btree_insert(200, NULL, 0, NULL, 0, btree);
  btree_insert(18, NULL, 0, NULL, 0, btree);
  btree_insert(2, NULL, 0, NULL, 0, btree);
  btree_insert(24, NULL, 0, NULL, 0, btree);
  btree_insert(8, NULL, 0, NULL, 0, btree);
  btree_insert(297, NULL, 0, NULL, 0, btree);
  btree_insert(2111, NULL, 0, NULL, 0, btree);
  btree_insert(4572, NULL, 0, NULL, 0, btree);
  btree_insert(23345, NULL, 0, NULL, 0, btree);
  btree_insert(254, NULL, 0, NULL, 0, btree);
  btree_insert(2245, NULL, 0, NULL, 0, btree);
  btree_insert(26573, NULL, 0, NULL, 0, btree);

  btree_delete(42, btree);
  btree_delete(2, btree);
  btree_delete(1350, btree);
  btree_delete(200, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(100, btree);
  btree_delete(18, btree);
  btree_delete(434, btree);
  btree_delete(10, btree);
  btree_delete(1021, btree);
  btree_delete(26573, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(297, btree);
  btree_delete(4572, btree);
  btree_delete(451, btree);
  btree_delete(1300, btree);
  btree_delete(9990, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(8, btree);
  btree_delete(2111, btree);
  btree_delete(422, btree);
  btree_delete(1400, btree);
  btree_delete(1070, btree);
  btree_delete(2245, btree);
  btree_delete(23345, btree);
  btree_delete(4, btree);
  btree_delete(3, btree);
  btree_delete(254, btree);
  btree_delete(309, btree);
  // print_btree(btree->root, NULL, btree);
  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(1, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(33, btree);
  btree_delete(331, btree);
  btree_delete(983, btree);
  btree_delete(1530, btree);
  btree_delete(461, btree);

  assert_true(check_parent_btree(btree->root, NULL, btree) == 0);
  btree_delete(24, btree);
  btree_delete(41, btree);
  assert_true(btree->root == NULL);
  close_store(btree);
}

// Test some of each function
static void integration_test(void **state) {
  struct btree *btree = (struct btree *)init_store(3, 2);
  char *sample_text_50 = "HELLO THERE";
  char *sample_text_30 =
      "Lorem ipsum dolor sit amet consectetur, adipisicing elit. Aut magnam a "
      "quia maxime ipsa sed, quos dolores est. Quo ducimus vel quisquam? "
      "Totam, assumenda ipsam. Quibusdam dolorum hic nesciunt nisi ab "
      "laboriosam, ducimus perferendis eligendi! Nobis ipsa eaque id nostrum "
      "fugiat assumenda, praesentium mollitia, obcaecati exercitationem "
      "tenetur corporis culpa minima cupiditate unde ad. Deserunt quasi quidem "
      "nostrum temporibus ad labore veniam blanditiis, eos facilis accusamus "
      "sequi sapiente eligendi omnis iure dicta illum dolor fugiat porro cum "
      "adipisci eveniet voluptatem molestias, possimus quos? Reprehenderit "
      "beatae inventore doloribus provident minima corporis quae libero atque "
      "blanditiis vitae quasi eaque aperiam eum optio nulla ullam dicta "
      "aliquam quod eligendi ipsam nobis expedita maiores, porro rerum. "
      "Doloremque expedita nihil iste deleniti quaerat enim porro, commodi "
      "quia, aperiam quis sint accusantium fugit animi facere voluptatibus "
      "voluptates facilis rerum cum! Natus sint explicabo beatae perferendis, "
      "ab similique illum quaerat recusandae eligendi exercitationem iure esse "
      "maxime? Mollitia at iste non earum itaque cum tempore optio nam velit, "
      "quia incidunt molestiae omnis repellat eos voluptatum, quae et "
      "doloremque illum ipsa accusamus maxime impedit. Similique nam sint "
      "ullam pariatur. Iste molestiae iure hic nobis et illo nisi nostrum "
      "expedita officia deleniti ex dolores laborum, quibusdam, id inventore. "
      "Qui ullam suscipit, doloremque nesciunt libero optio distinctio iusto "
      "laborum tempora voluptatibus eius similique vero. Sit amet explicabo "
      "reiciendis possimus illum nulla maxime pariatur voluptatibus incidunt "
      "a, sint provident alias atque enim asperiores necessitatibus "
      "consequuntur perferendis, animi quis. Voluptatem incidunt dolores "
      "tenetur aut natus? Exercitationem tempore illo possimus reiciendis quos "
      "iste at consectetur neque atque, officiis eligendi nostrum praesentium, "
      "fugit ratione ea error laborum sed laboriosam. Amet deleniti odio cum "
      "dolore magnam dolores itaque expedita libero modi incidunt corrupti "
      "veniam dicta maiores, animi molestiae voluptas repudiandae voluptatem "
      "quia deserunt fuga suscipit at dignissimos, ducimus sed. Harum omnis "
      "possimus necessitatibus, blanditiis perspiciatis facere cum enim quia "
      "rem neque aliquid tenetur veniam corrupti fugiat ullam! Recusandae, "
      "totam iure vero labore quibusdam, quis voluptatum vel error vitae "
      "mollitia doloribus nihil, pariatur iusto alias fugiat tenetur nostrum "
      "necessitatibus. Vero omnis quos quaerat cumque eius repellat odit "
      "aliquam, maxime possimus magnam perferendis quas. Explicabo aut "
      "voluptatibus laborum illo a fugit sed quia? Officia fugiat quo libero "
      "nesciunt? Quidem, expedita consequuntur magnam ratione voluptatibus "
      "repellat fugiat aperiam minus ad iusto aut asperiores distinctio "
      "debitis dicta a vitae, et architecto pariatur dolorem doloremque "
      "impedit nulla consequatur voluptates quod! Inventore repellendus, "
      "mollitia accusamus ex reiciendis nemo magnam, dolores expedita alias "
      "aspernatur non tempora labore tempore accusantium placeat officiis quia "
      "aliquid nam facilis. Ab maxime neque, consectetur libero doloribus "
      "ipsam illo dolorem aut perspiciatis laborum illum assumenda deleniti "
      "voluptate dolores at quisquam dignissimos, aliquam, minus omnis "
      "accusantium. Commodi reprehenderit nisi consectetur animi! Labore "
      "officiis voluptates iste quos suscipit quam, molestiae accusamus "
      "nesciunt adipisci, eum iure praesentium amet tenetur quae iusto omnis "
      "vero. Sit quasi omnis velit libero veniam reiciendis nemo aut placeat, "
      "iure officiis iste harum nobis ut cum nulla ullam incidunt commodi, "
      "illo quod accusamus. Sit, dolores dignissimos totam architecto corporis "
      "harum neque quae obcaecati aperiam a necessitatibus, assumenda "
      "temporibus quibusdam.";
  char *sample_text_12 = "my nemjeff";
  uint32_t encrypt_50[4] = {9, 23, 34, 14};
  uint32_t encrypt_30[4] = {96, 233, 334, 124};
  uint32_t encrypt_12[4] = {65, 13423, 24334, 2414};
  uint64_t nonce_1 = 8327459823;
  uint64_t nonce_2 = 2464236738;
  btree_insert(45, NULL, 0, NULL, 0, btree);
  btree_insert(40, NULL, 0, NULL, 0, btree);
  btree_insert(50, sample_text_50, 12, encrypt_50, nonce_1, btree);
  btree_insert(20, NULL, 0, NULL, 0, btree);
  btree_insert(10, NULL, 0, NULL, 0, btree);
  btree_insert(15, NULL, 0, NULL, 0, btree);
  btree_insert(30, sample_text_30, 3767, encrypt_30, nonce_1, btree);
  btree_insert(25, NULL, 0, NULL, 0, btree);
  btree_insert(16, NULL, 0, NULL, 0, btree);
  btree_insert(17, NULL, 0, NULL, 0, btree);
  btree_insert(12, sample_text_12, 11, encrypt_12, nonce_2, btree);
  check_parent_btree(btree->root, NULL, btree);

  btree_insert(18, NULL, 0, NULL, 0, btree);
  // print_btree(btree->root, NULL, btree);
  void *output = malloc(12);
  assert_true(btree_decrypt(50, output, btree) == 0);
  assert_true(strcmp(output, sample_text_50) == 0);
  free(output);

  struct info *found = malloc(sizeof(struct info));
  assert_true(btree_retrieve(50, found, btree) == 0);
  assert_true(found->data != NULL);
  assert_true(found->key[0] == 9);
  assert_true(found->key[1] == 23);
  assert_true(found->key[2] == 34);
  assert_true(found->key[3] == 14);
  assert_true(found->nonce == 8327459823);
  assert_true(found->size == 12);
  free(found);

  void *output_2 = malloc(3767);
  assert_true(btree_decrypt(30, output_2, btree) == 0);
  assert_true(strcmp(output_2, sample_text_30) == 0);
  free(output_2);

  struct info *found_2 = malloc(sizeof(struct info));
  assert_true(btree_retrieve(30, found_2, btree) == 0);
  assert_true(found_2->data != NULL);
  assert_true(found_2->key[0] == 96);
  assert_true(found_2->key[1] == 233);
  assert_true(found_2->key[2] == 334);
  assert_true(found_2->key[3] == 124);
  assert_true(found_2->nonce == 8327459823);
  assert_true(found_2->size == 3767);
  free(found_2);

  void *output_3 = malloc(18);
  assert_true(btree_decrypt(12, output_3, btree) == 0);
  assert_true(strcmp(output_3, sample_text_12) == 0);
  free(output_3);

  struct info *found_3 = malloc(sizeof(struct info));
  assert_true(btree_retrieve(12, found_3, btree) == 0);
  assert_true(found_3->data != NULL);
  assert_true(found_3->key[0] == 65);
  assert_true(found_3->key[1] == 13423);
  assert_true(found_3->key[2] == 24334);
  assert_true(found_3->key[3] == 2414);
  assert_true(found_3->nonce == 2464236738);
  assert_true(found_3->size == 11);

  free(found_3);

  assert_true(btree_insert(20, NULL, 0, NULL, 0, btree) == 1);
  assert_true(btree_insert(25, NULL, 0, NULL, 0, btree) == 1);
  btree_delete(50, btree);
  btree_delete(10, btree);
  btree_delete(12, btree);

  void *sample_output = malloc(10000);
  struct info *found_sample = malloc(10000);
  assert_true(btree_decrypt(50, sample_output, btree) == 1);
  assert_true(btree_decrypt(10, sample_output, btree) == 1);
  assert_true(btree_decrypt(12, sample_output, btree) == 1);

  assert_true(btree_delete(50, btree) == 1);
  assert_true(btree_delete(10, btree) == 1);
  assert_true(btree_delete(12, btree) == 1);

  assert_true(btree_retrieve(50, found_sample, btree) == 1);
  assert_true(btree_retrieve(10, found_sample, btree) == 1);
  assert_true(btree_retrieve(12, found_sample, btree) == 1);

  free(sample_output);
  free(found_sample);

  close_store(btree);
}

int main() {
  // Your own testing code here
  const struct CMUnitTest tests[] = {
      cmocka_unit_test(init_store_test),
      cmocka_unit_test(tea_ctr_test),
      cmocka_unit_test_setup_teardown(hard_coded_tree_test_print_all,
                                      hard_coded_tree_setup, NULL),
      cmocka_unit_test_setup_teardown(hard_coded_tree_export_test,
                                      hard_coded_tree_setup, NULL),
      cmocka_unit_test_setup_teardown(search_test_hard_code,
                                      hard_coded_tree_setup, NULL),
      cmocka_unit_test_setup_teardown(search_node_to_put_key_test,
                                      hard_coded_tree_setup, NULL),
      cmocka_unit_test(insert_empty_tree_test),
      cmocka_unit_test(insert_to_root_no_split),
      cmocka_unit_test(insert_to_root_split),
      cmocka_unit_test(spam_insert_test),
      cmocka_unit_test(encrypt_insert_decrypt_single),
      cmocka_unit_test(delete_leaf_test_no_merge),
      cmocka_unit_test(search_max_test),
      cmocka_unit_test(delete_internal_no_merge),
      cmocka_unit_test(delete_leaf_steal_left_sibling_once),
      cmocka_unit_test(delete_leaf_steal_right_sibling_once),
      cmocka_unit_test(merge_left_once_test),
      cmocka_unit_test(merge_right_once_test),
      cmocka_unit_test(test_non_recursive_delete_multiple),
      cmocka_unit_test(simple_recursive_delete_left),
      cmocka_unit_test(simple_recursive_delete_right),
      cmocka_unit_test(massive_deletion_recursion_to_empty),
      cmocka_unit_test(delete_only_node),
      cmocka_unit_test(massive_deletion_recursion_to_empty_bf_4),
      cmocka_unit_test(massive_deletion_recursion_to_empty_bf_7),
      cmocka_unit_test(integration_test)};

  return cmocka_run_group_tests(tests, NULL, NULL);
}
