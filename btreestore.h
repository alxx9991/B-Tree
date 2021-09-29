#ifndef BTREESTORE_H
#define BTREESTORE_H

#include <math.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DELTA 2654435769
#define SUM 3722896384

// Global tree which includes the branching factor, the root node and the number
// of processors
struct btree {
  struct btree_node* root;
  uint16_t branching;
  uint8_t n_processors;
  pthread_mutex_t lock;
};

struct btree_node {
  uint16_t num_keys;
  struct key_data* key_value_pairs;
  struct btree_node* parent;  // If parent is NULL, it is the head node
  struct btree_node* children;
  uint8_t is_leaf;
};

// Provided info
struct info {
  uint32_t size;
  uint32_t key[4];
  uint64_t nonce;
  void* data;
};

// Node for exporting btree
struct node {
  uint32_t* keys;
  uint16_t num_keys;
};

// Represents a key-value pair within a node
struct key_data {
  uint32_t key;
  void* encrypted_data;
  uint32_t encryption_key[4];
  uint64_t nonce;
  uint32_t size;
};

void print_node(struct btree_node* node);

void print_btree(struct btree_node* root, struct btree_node* caller,
                 struct btree* btree);

int check_parent_btree(struct btree_node* root, struct btree_node* caller,
                       struct btree* btree);

void free_btree(struct btree_node* root);

void count_nodes(struct btree_node* root, int* count);

struct key_data* search_key_data(struct btree_node* root, uint32_t key);

struct btree_node* search_node_to_put_key(struct btree_node* root,
                                          uint32_t key);

uint32_t search_key_in_node(struct btree_node* node, uint32_t key);

void split_node(struct key_data* key_to_promote,
                struct btree_node* node_to_split, struct btree* btree);

void copy_key_data(struct key_data* destination, struct key_data* source);

struct btree_node* search_max_node(struct btree_node* root);

int recursive_delete(struct btree_node* node_to_delete, struct btree* btree);

int key_swap(struct btree_node* node_to_delete, struct btree* btree,
             uint32_t index_in_children_list);

struct btree_node* merge(struct btree_node* node_to_delete_from,
                         uint32_t index_in_children_list);

void* init_store(uint16_t branching, uint8_t n_processors);

void close_store(void* helper);

int btree_insert(uint32_t key, void* plaintext, size_t count,
                 uint32_t encryption_key[4], uint64_t nonce, void* helper);

int btree_retrieve(uint32_t key, struct info* found, void* helper);

int btree_decrypt(uint32_t key, void* output, void* helper);

int btree_delete(uint32_t key, void* helper);

uint64_t btree_export(void* helper, struct node** list);

void encrypt_tea(uint32_t plain[2], uint32_t cipher[2], uint32_t key[4]);

void decrypt_tea(uint32_t cipher[2], uint32_t plain[2], uint32_t key[4]);

void encrypt_tea_ctr(uint64_t* plain, uint32_t key[4], uint64_t nonce,
                     uint64_t* cipher, uint32_t num_blocks);

void decrypt_tea_ctr(uint64_t* cipher, uint32_t key[4], uint64_t nonce,
                     uint64_t* plain, uint32_t num_blocks);

#endif
