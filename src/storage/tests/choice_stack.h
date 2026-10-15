#pragma once

#include <cstddef>
#include <stdexcept>
#include <utility>
#include <vector>

namespace kwaque::storage::testing {

// Runs a test body once for every sequence of choices it can make. The body
// asks for each choice as it comes to it; rewind() then moves to the next
// sequence, depth first, and says whether there is one:
//
//   choice_stack choices;
//   do {
//       const int cut = choices.choose_up_to(last_cut);
//       const bool survives = choices.choose_bool();
//       ...
//   } while (choices.rewind());
//
// The body must ask for the same choices whenever it is given the same
// answers. The answers of the run that failed are the whole of what is
// needed to run it again.
class choice_stack final {
public:
    // Chooses a non-negative integer. Followed by mark_last_choice() when
    // the answer is the largest one to try. For a number of answers that is
    // not known before the first is given; choose_up_to() is easier to use
    // otherwise.
    int choose() {
        if (next_frame_ >= choices_.size()) {
            choices_.emplace_back(0, false);
        } else if (next_frame_ + 1 == choices_.size()) {
            choices_.back().first += 1;
        }
        return choices_[next_frame_++].first;
    }
    // Marks the last choose() as fully explored.
    void mark_last_choice() {
        if (
          next_frame_ == 0
          || (next_frame_ != choices_.size() && !choices_[next_frame_ - 1].second))
            throw std::logic_error{"a choice was marked out of turn"};
        choices_[next_frame_ - 1].second = true;
    }
    // Chooses an integer in [0, n].
    int choose_up_to(int n) {
        const int result = choose();
        if (result >= n) mark_last_choice();
        return result;
    }
    bool choose_bool() { return choose_up_to(1) != 0; }
    // Moves to the next run of the body; false when every one was made.
    bool rewind() {
        while (!choices_.empty() && choices_.back().second)
            choices_.pop_back();
        next_frame_ = 0;
        return !choices_.empty();
    }
    // The answers given so far in this run, in the order they were asked.
    [[nodiscard]] std::vector<int> answers() const {
        std::vector<int> given;
        given.reserve(next_frame_);
        for (std::size_t frame = 0; frame != next_frame_; ++frame)
            given.push_back(choices_[frame].first);
        return given;
    }

private:
    std::vector<std::pair<int, bool>> choices_;
    std::size_t next_frame_{0};
};

} // namespace kwaque::storage::testing
