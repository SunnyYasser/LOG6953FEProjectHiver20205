#ifndef VFENGINE_PARSER_DEBUG_HH
#define VFENGINE_PARSER_DEBUG_HH

namespace VFEngine {
    class QueryParser;
    class ParserDebugUtility
    {
    public:
        static void print_logical_plan (const QueryParser* parser);
    };
}

#endif
