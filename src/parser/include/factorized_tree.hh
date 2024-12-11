#ifndef VFENGINE_FACTORIZED_TREE_HH
#define VFENGINE_FACTORIZED_TREE_HH

#include "logical_pipeline_element.hh"
#include "factorized_tree_element.hh"

namespace VFEngine {
    class FactorizedTree {
    public:
        explicit FactorizedTree(const std::vector<LogicalPipelineElement> &logical_plan);
        [[nodiscard]] std::shared_ptr<FactorizedTreeElement> build_tree();

    private:
        std::vector<LogicalPipelineElement> _logical_plan;
        std::shared_ptr<FactorizedTreeElement> insert(const std::shared_ptr<FactorizedTreeElement> &root,
                                                      const LogicalPipelineElement &node) const;
        [[nodiscard]] size_t get_max_depth(const std::shared_ptr<FactorizedTreeElement> &node) const;
        [[nodiscard]] std::shared_ptr<FactorizedTreeElement>
        find_last_non_leaf_node(const std::shared_ptr<FactorizedTreeElement> &node) const;
        [[nodiscard]] std::shared_ptr<FactorizedTreeElement> create_node(const LogicalPipelineElement &ele,
                                                                         bool is_first) const;
    };
} // namespace VFEngine

#endif
