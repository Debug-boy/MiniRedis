#ifndef MINIREDIS_POLICY_HPP
#define MINIREDIS_POLICY_HPP

#include <chrono>

namespace MiniRedis {

    struct DumpTriggerPolicy {
        std::chrono::milliseconds millisecond;
        uint64_t dirtyThreshold;
        DumpTriggerPolicy() : millisecond(0), dirtyThreshold(0){}
        DumpTriggerPolicy(const std::chrono::milliseconds m, const uint64_t d) : millisecond(m),dirtyThreshold(d){}
    };

    inline std::ostream& operator <<(std::ostream& stream,const DumpTriggerPolicy&cr) {
        return stream  << "(" << cr.millisecond.count() << "," << cr.dirtyThreshold << ")";
    }

}

#endif //MINIREDIS_POLICY_HPP