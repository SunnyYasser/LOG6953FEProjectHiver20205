//
// Created by Sunny on 21-08-2024.
//
#include "include/debug_enabled.hh"

static bool _is_debug_enabled = false;
static bool _is_memory_debug_enabled = false;
static bool _is_operator_debug_enabled = false;


bool is_debug_enabled() { return _is_debug_enabled; }

void enable_debug() { _is_debug_enabled = true; }

void disable_debug() { _is_debug_enabled = false; }

void enable_memory_debug() { _is_memory_debug_enabled = true; }

void disable_memory_debug() { _is_memory_debug_enabled = false; }

void enable_operator_debug() { _is_operator_debug_enabled = true; }

void disable_operator_debug() { _is_operator_debug_enabled = false; }

bool is_operator_debug_enabled() { return _is_operator_debug_enabled; }

bool is_memory_debug_enabled() { return _is_memory_debug_enabled; }
