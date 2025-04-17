#include "include/factorized_tree_element.hh"
#include <iostream>
namespace VFEngine {
    void FactorizedTreeElement::set_value_ptr(const Vector *value) { _value = value; }
    bool FactorizedTreeElement::is_leaf() const { return _children.empty(); }

    void print_tree_level(const std::vector<std::shared_ptr<FactorizedTreeElement>> &nodes, const int depth) {
        for (int i = 0; i < depth; ++i) {
            std::cout << "  ";
        }

        for (const auto &node: nodes) {
            std::cout << node->_attribute << " ";
        }
        std::cout << std::endl;

        // Collect all children for the next level
        std::vector<std::shared_ptr<FactorizedTreeElement>> next_level;
        for (const auto &node: nodes) {
            next_level.insert(next_level.end(), node->_children.begin(), node->_children.end());
        }

        if (!next_level.empty()) {
            print_tree_level(next_level, depth + 1);
        }
    }

    void print_tree_level(const FactorizedTreeElement *root) {
        std::cout << root->_attribute << std::endl;
        std::vector<std::shared_ptr<FactorizedTreeElement>> next_level;
        next_level.insert(next_level.end(), root->_children.begin(), root->_children.end());
        print_tree_level(next_level, 1);
    }

    void FactorizedTreeElement::print_tree() const { print_tree_level(this); }


} // namespace VFEngine
