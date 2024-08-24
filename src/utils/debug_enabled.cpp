//
// Created by Sunny on 21-08-2024.
//
#include "include/debug_enabled.h"

static bool _is_debug_enabled = false;

bool is_debug_enabled ()
{
    return _is_debug_enabled;
}

void enable_debug ()
{
    _is_debug_enabled = true;
}

void disable_debug ()
{
    _is_debug_enabled = false;
}