#pragma once

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace kwaque::storage::testing {

// What the driver of one shard's storage did and what it was told, and what
// that permits a later read or a restart to show. Nothing here comes from
// the engine but the results it handed its caller: no cursor of its own, no
// checkpoint and no scan of it supplies an expectation.
//
// A request reported durable is read back exactly, in its segment's order,
// until its segment's removal is reported. A request with no result may be
// there or not, and never makes one that was reported durable wrong. A
// result that arrives once a crash has begun is not a result. What is not
// an order of requests, a segment's name, is kept as the set of states its
// history allows: a request with no result widens the set and what is seen
// later narrows it.
//
// What it is told can be written down, a line a call, and told to another
// ledger later: one that hears the same lines in the same order knows the
// same. That is how what a process was told outlives the process.
class engine_ledger final {
public:
    using segment_id = std::uint64_t;
    using request_id = std::size_t;

    enum class verdict : std::uint8_t {
        holds,
        // A request reported durable is not read back.
        lost,
        // Its bytes are read back and are not the ones offered.
        altered,
        // Requests are read back out of their segment's order.
        reordered,
        // Something is read that was never accepted there.
        foreign,
        // Something is read at or past what its reader may see.
        exposed,
        // A segment is read, or named, after its removal was reported.
        resurrected,
        // A segment is under a name no outcome of its history allows.
        misnamed,
        // A segment that failed, sealed or was found by a restart took
        // another request.
        unfenced,
        // No crash and no fault, and a request was left without a result.
        stalled,
        // The driver's own calls contradict each other.
        misuse,
    };
    // Where a segment's directory is.
    enum class name : std::uint8_t { absent = 1, own = 2, deleting = 4 };
    // How a restart's catalog lists a segment.
    enum class listing : std::uint8_t { none, kept, expiring };
    struct read_batch final {
        std::uint64_t logical;
        std::string_view bytes;
    };

    // What a line is to the ledger that hears it.
    enum class line : std::uint8_t {
        // One of the calls below, made.
        told,
        // An answer that was not passed on.
        withheld,
        // A line its driver put among the ledger's own.
        note,
        // Nothing the ledger writes.
        unknown,
    };

    // The first verdict that was not `holds`, and what it was about.
    [[nodiscard]] verdict first() const noexcept { return first_; }
    [[nodiscard]] const std::string& about() const noexcept { return about_; }

    // From here on every call that tells the ledger something is written
    // down first, a line a call.
    void record(std::function<void(const std::string&)> lines) {
        lines_ = std::move(lines);
    }
    // A line of the driver's own, kept in its place among the ledger's.
    void note(std::string_view text) {
        if (lines_) lines_("note " + std::string{text});
    }
    // Makes the call one recorded line says. With `asked_only`, what was
    // asked for is told and no answer to it is: a ledger that hears a
    // record that way knows what a caller knows who never got the answers.
    line hear(std::string_view text, bool asked_only = false) {
        const auto verb = word(text);
        if (verb == "note") return line::note;
        const auto answer = [asked_only] {
            return asked_only ? line::withheld : line::told;
        };
        if (verb == "faulted" || verb == "crash" || verb == "restart") {
            if (!text.empty()) return line::unknown;
            if (verb == "faulted") {
                faulted();
            } else if (verb == "crash") {
                crash();
            } else {
                restart();
            }
            return line::told;
        }
        const auto first = number(word(text));
        if (!first) return line::unknown;
        if (verb == "offer") {
            const auto logical = number(word(text));
            auto bytes = unhex(word(text));
            if (!logical || !bytes || !text.empty()) return line::unknown;
            static_cast<void>(offer(*first, *logical, std::move(*bytes)));
            return line::told;
        }
        if (verb == "durable") {
            const auto at = number(word(text));
            auto block = unhex(word(text));
            if (!at || !block || !text.empty()) return line::unknown;
            if (!asked_only) durable(*first, *at, std::move(*block));
            return answer();
        }
        if (verb == "create_refused") {
            const auto nothing = number(word(text));
            if (!nothing || *nothing > 1 || !text.empty()) return line::unknown;
            if (!asked_only) create_refused(*first, *nothing == 1);
            return answer();
        }
        if (!text.empty()) return line::unknown;
        using asked = void (engine_ledger::*)(std::uint64_t);
        struct call final {
            std::string_view verb;
            asked made;
            bool answers;
        };
        static constexpr std::array calls{
          call{"create_asked", &engine_ledger::create_asked, false},
          call{"created", &engine_ledger::created, true},
          call{"accepted", &engine_ledger::accepted, true},
          call{"refused", &engine_ledger::refused, true},
          call{"failed", &engine_ledger::failed, true},
          call{"seal_asked", &engine_ledger::seal_asked, false},
          call{"sealed", &engine_ledger::sealed, true},
          call{"seal_failed", &engine_ledger::seal_failed, true},
          call{"removal_asked", &engine_ledger::removal_asked, false},
          call{"removed", &engine_ledger::removed, true},
        };
        for (const auto& one : calls) {
            if (one.verb != verb) continue;
            if (one.answers && asked_only) return line::withheld;
            (this->*one.made)(*first);
            return line::told;
        }
        return line::unknown;
    }

    // A creation: asked for, then answered. One that made nothing leaves
    // nothing; one that failed otherwise may have left its directory.
    void create_asked(segment_id segment) {
        write("create_asked", segment);
        auto [found, inserted] = segments_.try_emplace(segment);
        if (!inserted) {
            fail(verdict::misuse, "segment created twice");
            return;
        }
        found->second.names = bits(name::absent) | bits(name::own);
    }
    void created(segment_id segment) {
        write("created", segment);
        if (auto* found = live(segment)) {
            found->names = bits(name::own);
            found->made = true;
        }
    }
    void create_refused(segment_id segment, bool made_nothing) {
        write("create_refused", segment, made_nothing ? 1U : 0U);
        if (auto* found = live(segment)) {
            if (made_nothing) found->names = bits(name::absent);
            found->failed = true;
        }
    }

    // One request, from the offer to its result. `logical` is where its
    // records begin. Requests take their order in their segment in the
    // order they are offered, in rising order of `logical`; one that was
    // offered may have been accepted whatever its caller came to hear.
    [[nodiscard]] request_id
    offer(segment_id segment, std::uint64_t logical, std::string bytes) {
        write("offer", segment, logical, &bytes);
        const auto id = requests_.size();
        auto* found = known(segment);
        const bool fenced
          = found && (found->failed || found->sealing || found->recovered);
        requests_.push_back(
          {segment,
           logical,
           std::move(bytes),
           {},
           0,
           state::offered,
           life_,
           fenced});
        if (!found) {
            fail(verdict::misuse, "request to no segment");
        } else if (
          !found->order.empty()
          && requests_[found->order.back()].logical >= logical) {
            fail(verdict::misuse, "requests not in rising order");
        } else {
            found->order.push_back(id);
        }
        return id;
    }
    void accepted(request_id id) {
        write("accepted", id);
        auto* found = pending(id, state::offered);
        if (!found) return;
        if (found->fenced) {
            fail(verdict::unfenced, found->segment);
            return;
        }
        found->held = state::accepted;
    }
    void refused(request_id id) {
        write("refused", id);
        if (auto* found = pending(id, state::offered))
            found->held = state::refused;
    }
    // Reported durable, with the block the receipt named: where it begins
    // in its segment's data file and the bytes it holds there.
    void durable(request_id id, std::uint64_t at, std::string block) {
        write("durable", id, at, &block);
        auto* found = pending(id, state::accepted);
        if (!found) return;
        found->at = at;
        found->block = std::move(block);
        found->held = state::durable;
    }
    // Reported not durable after it was accepted: what it left is not
    // known, and its segment takes nothing more.
    void failed(request_id id) {
        write("failed", id);
        if (auto* found = pending(id, state::accepted)) {
            found->held = state::failed;
            if (auto* segment = known(found->segment)) segment->failed = true;
        }
    }

    // A seal: asked for, then answered. A seal that failed may have sealed.
    void seal_asked(segment_id segment) {
        write("seal_asked", segment);
        if (auto* found = live(segment)) found->sealing = true;
    }
    void sealed(segment_id segment) {
        write("sealed", segment);
        if (auto* found = live(segment)) found->sealed = true;
    }
    void seal_failed(segment_id segment) {
        write("seal_failed", segment);
        if (auto* found = live(segment)) found->failed = true;
    }

    // A removal: asked for, then reported gone. One that is not reported
    // gone stands anywhere on its way.
    void removal_asked(segment_id segment) {
        write("removal_asked", segment);
        if (auto* found = live(segment)) {
            found->expiring = true;
            found->names |= bits(name::deleting) | bits(name::absent);
        }
    }
    void removed(segment_id segment) {
        write("removed", segment);
        if (auto* found = live(segment)) {
            found->names = bits(name::absent);
            found->gone = true;
        }
    }

    // A fault was injected: what follows in this life need not make
    // progress.
    void faulted() {
        write("faulted");
        faulted_ = true;
    }
    // A crash begins: no result that arrives from here on is one.
    void crash() {
        write("crash");
        crashed_ = true;
    }
    // A start is about to be given its catalog. What it lists as expiring
    // may go on to its deleting name and to nothing; a segment that was
    // appended to takes nothing more.
    void restart() {
        write("restart");
        crashed_ = false;
        faulted_ = false;
        ++life_;
        for (auto& [id, segment] : segments_) {
            if (listed(id) == listing::expiring) {
                segment.expiring = true;
                segment.names |= bits(name::deleting) | bits(name::absent);
            }
            if (segment.made) segment.recovered = true;
        }
    }

    // How the next start's catalog lists a segment: one reported gone is
    // not listed; one whose removal was asked for, or whose creation was
    // not answered or failed, is listed as expiring; any other as kept.
    [[nodiscard]] listing listed(segment_id segment) const noexcept {
        const auto found = segments_.find(segment);
        if (found == segments_.end() || found->second.gone)
            return listing::none;
        // Nothing of it was ever made.
        if (!found->second.made && found->second.names == bits(name::absent))
            return listing::none;
        if (found->second.expiring || !found->second.made)
            return listing::expiring;
        return listing::kept;
    }
    // Whether its removal is authorized as that of a segment not sealed.
    [[nodiscard]] bool unsealed(segment_id segment) const noexcept {
        const auto found = segments_.find(segment);
        return found != segments_.end() && !found->second.sealed
               && !found->second.sealing;
    }
    [[nodiscard]] std::vector<segment_id> segments() const {
        std::vector<segment_id> all;
        all.reserve(segments_.size());
        for (const auto& [id, segment] : segments_)
            all.push_back(id);
        return all;
    }
    // The requests of a segment that were reported durable, in its order.
    [[nodiscard]] std::size_t acknowledged(segment_id segment) const noexcept {
        const auto found = segments_.find(segment);
        if (found == segments_.end()) return 0;
        return static_cast<std::size_t>(
          std::ranges::count_if(found->second.order, [this](request_id id) {
              return requests_[id].held == state::durable;
          }));
    }

    // Where a segment's directory was seen.
    verdict named(segment_id segment, name seen) {
        auto* found = known(segment);
        if (!found) return fail(verdict::misuse, "name of no segment");
        if ((found->names & bits(seen)) == 0)
            return fail(
              found->gone ? verdict::resurrected : verdict::misnamed, segment);
        found->names = bits(seen);
        return verdict::holds;
    }

    // What a reader was given of a segment from its beginning, in the order
    // it was given, by one that may see what begins below `visible`. With
    // `whole`, the read reached the end of what the segment holds there:
    // every request reported durable below `visible` is among it.
    verdict read_back(
      segment_id segment,
      std::span<const read_batch> batches,
      std::uint64_t visible,
      bool whole) {
        const auto* found = known(segment);
        if (!found) return fail(verdict::misuse, "read of no segment");
        if (found->gone)
            return batches.empty() ? verdict::holds
                                   : fail(verdict::resurrected, segment);
        std::size_t next = 0;
        for (const auto& batch : batches) {
            if (batch.logical >= visible)
                return fail(verdict::exposed, segment);
            // Its place in the segment's order: past everything read so
            // far, or it was read out of order, or it is no request here.
            auto place = next;
            while (place != found->order.size()
                   && requests_[found->order[place]].logical != batch.logical)
                ++place;
            if (place == found->order.size()) {
                const bool earlier = std::ranges::any_of(
                  found->order.begin(),
                  found->order.begin() + static_cast<std::ptrdiff_t>(next),
                  [&](request_id id) {
                      return requests_[id].logical == batch.logical;
                  });
                return fail(
                  earlier ? verdict::reordered : verdict::foreign, segment);
            }
            if (requests_[found->order[place]].held == state::refused)
                return fail(verdict::foreign, found->order[place]);
            // A request reported durable that the read stepped over.
            for (auto skipped = next; skipped != place; ++skipped)
                if (requests_[found->order[skipped]].held == state::durable)
                    return fail(verdict::lost, found->order[skipped]);
            const auto& request = requests_[found->order[place]];
            if (request.bytes != batch.bytes)
                return fail(verdict::altered, found->order[place]);
            next = place + 1;
        }
        if (whole)
            for (auto rest = next; rest != found->order.size(); ++rest) {
                const auto& request = requests_[found->order[rest]];
                if (request.held == state::durable && request.logical < visible)
                    return fail(verdict::lost, found->order[rest]);
            }
        return verdict::holds;
    }

    // A segment's data file as a device holds it: every request reported
    // durable has its block where its receipt said, byte for byte, and the
    // blocks lie in the segment's order.
    verdict image_back(segment_id segment, std::string_view file) {
        const auto* found = known(segment);
        if (!found) return fail(verdict::misuse, "image of no segment");
        std::uint64_t end = 0;
        for (const auto id : found->order) {
            const auto& request = requests_[id];
            if (request.held != state::durable) continue;
            if (request.at < end) return fail(verdict::reordered, id);
            end = request.at + request.block.size();
            if (
              request.at > file.size()
              || file.size() - request.at < request.block.size())
                return fail(verdict::lost, id);
            if (file.substr(request.at, request.block.size()) != request.block)
                return fail(verdict::altered, id);
        }
        return verdict::holds;
    }
    // A segment whose data file is not there to read.
    verdict image_missing(segment_id segment) {
        const auto* found = known(segment);
        if (!found) return fail(verdict::misuse, "image of no segment");
        if (found->expiring || found->gone) return verdict::holds;
        for (const auto id : found->order)
            if (requests_[id].held == state::durable)
                return fail(verdict::lost, id);
        return verdict::holds;
    }

    // With no crash and no fault in this life, every request offered in it
    // has its result.
    verdict progressed() {
        if (crashed_ || faulted_) return verdict::holds;
        for (request_id id = 0; id != requests_.size(); ++id) {
            const auto& request = requests_[id];
            if (
              request.life == life_
              && (request.held == state::offered || request.held == state::accepted))
                return fail(verdict::stalled, id);
        }
        return verdict::holds;
    }

private:
    enum class state : std::uint8_t {
        offered,
        refused,
        accepted,
        durable,
        failed,
    };
    struct request final {
        segment_id segment;
        std::uint64_t logical;
        std::string bytes;
        std::string block;
        std::uint64_t at;
        state held;
        std::uint32_t life;
        // Its segment took no request when it was offered.
        bool fenced;
    };
    struct segment final {
        // Every request offered to it, in the order they were offered.
        std::vector<request_id> order;
        std::uint8_t names{0};
        bool made{false}, failed{false}, sealing{false}, sealed{false},
          recovered{false}, expiring{false}, gone{false};
    };
    [[nodiscard]] static constexpr std::uint8_t bits(name value) noexcept {
        return static_cast<std::uint8_t>(value);
    }
    [[nodiscard]] segment* known(segment_id id) {
        const auto found = segments_.find(id);
        return found == segments_.end() ? nullptr : &found->second;
    }
    // A segment an answer is about, unless a crash has begun: then nothing
    // that arrives is an answer.
    [[nodiscard]] segment* live(segment_id id) {
        if (crashed_) return nullptr;
        auto* found = known(id);
        if (!found) fail(verdict::misuse, "answer about no segment");
        return found;
    }
    [[nodiscard]] request* pending(request_id id, state from) {
        if (crashed_) return nullptr;
        if (id >= requests_.size() || requests_[id].held != from) {
            fail(verdict::misuse, "request answered out of turn");
            return nullptr;
        }
        return &requests_[id];
    }
    verdict fail(verdict what, std::string about) {
        if (first_ == verdict::holds) {
            first_ = what;
            about_ = std::move(about);
        }
        return what;
    }
    verdict fail(verdict what, std::uint64_t subject) {
        return fail(what, std::to_string(subject));
    }

    // One call as a line: its name, its numbers, and its bytes in hex.
    void write(
      std::string_view verb,
      std::optional<std::uint64_t> first = std::nullopt,
      std::optional<std::uint64_t> second = std::nullopt,
      const std::string* bytes = nullptr) {
        if (!lines_) return;
        std::string text{verb};
        for (const auto& value : {first, second})
            if (value) text += " " + std::to_string(*value);
        if (bytes) {
            static constexpr std::string_view digits = "0123456789abcdef";
            text += bytes->empty() ? " -" : " ";
            for (const char letter : *bytes) {
                const auto byte = static_cast<unsigned char>(letter);
                text += digits[byte >> 4U];
                text += digits[byte & 15U];
            }
        }
        lines_(text);
    }
    // The next word of a line, taken off its front.
    [[nodiscard]] static std::string_view word(std::string_view& text) {
        const auto end = std::min(text.find(' '), text.size());
        const auto found = text.substr(0, end);
        text.remove_prefix(std::min(end + 1, text.size()));
        return found;
    }
    [[nodiscard]] static std::optional<std::uint64_t>
    number(std::string_view text) {
        std::uint64_t value = 0;
        const auto* end = text.data() + text.size();
        const auto parsed = std::from_chars(text.data(), end, value);
        if (text.empty() || parsed.ec != std::errc{} || parsed.ptr != end)
            return std::nullopt;
        return value;
    }
    [[nodiscard]] static std::optional<std::string>
    unhex(std::string_view text) {
        if (text == "-") return std::string{};
        if (text.empty() || text.size() % 2 != 0) return std::nullopt;
        const auto digit = [](char letter) -> int {
            if (letter >= '0' && letter <= '9') return letter - '0';
            if (letter >= 'a' && letter <= 'f') return letter - 'a' + 10;
            return -1;
        };
        std::string bytes;
        bytes.reserve(text.size() / 2);
        for (std::size_t i = 0; i != text.size(); i += 2) {
            const auto high = digit(text[i]);
            const auto low = digit(text[i + 1]);
            if (high < 0 || low < 0) return std::nullopt;
            bytes.push_back(static_cast<char>(high * 16 + low));
        }
        return bytes;
    }

    std::map<segment_id, segment> segments_;
    std::vector<request> requests_;
    std::uint32_t life_{0};
    bool crashed_{false}, faulted_{false};
    verdict first_{verdict::holds};
    std::string about_;
    std::function<void(const std::string&)> lines_;
};

// A verdict as one word, for a line a harness reads.
[[nodiscard]] constexpr std::string_view
verdict_name(engine_ledger::verdict verdict) noexcept {
    using enum engine_ledger::verdict;
    switch (verdict) {
    case holds:
        return "holds";
    case lost:
        return "lost";
    case altered:
        return "altered";
    case reordered:
        return "reordered";
    case foreign:
        return "foreign";
    case exposed:
        return "exposed";
    case resurrected:
        return "resurrected";
    case misnamed:
        return "misnamed";
    case unfenced:
        return "unfenced";
    case stalled:
        return "stalled";
    case misuse:
        return "misuse";
    }
    return "unknown";
}

} // namespace kwaque::storage::testing
