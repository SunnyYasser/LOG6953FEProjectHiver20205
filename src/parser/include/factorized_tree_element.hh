#ifndef VFENGINE_FACTORIZED_TREE_ELEMENT_HH
#define VFENGINE_FACTORIZED_TREE_ELEMENT_HH

#include <string>
#include <vector>
#include "../../memory/include/vector.hh"

namespace VFEngine {
    class FactorizedTreeElement {
    public:
        FactorizedTreeElement(const std::string &attribute, const Vector *value) :
            _attribute(attribute), _value(value){};

        explicit FactorizedTreeElement(const std::string &attribute) : _attribute(attribute), _value(nullptr){};

        void set_value_ptr(const Vector *value);
        [[nodiscard]] bool is_leaf() const;
        void print_tree() const;

        std::weak_ptr<FactorizedTreeElement> _parent; // Use weak_ptr to prevent circular reference
        std::string _attribute;
        const Vector *_value;
        std::vector<std::shared_ptr<FactorizedTreeElement>> _children;
    };

} // namespace VFEngine
#endif
