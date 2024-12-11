#ifndef VFENGINE_DEBUG_ENABLED_H
#define VFENGINE_DEBUG_ENABLED_H

void enable_debug();
void disable_debug();
void enable_operator_debug();
void enable_memory_debug();
void enable_parser_debug();
bool is_debug_enabled();
bool is_memory_debug_enabled();
bool is_operator_debug_enabled();
bool is_parser_debug_enabled();

#endif
