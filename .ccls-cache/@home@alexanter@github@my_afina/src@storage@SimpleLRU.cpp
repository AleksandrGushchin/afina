#include "SimpleLRU.h"

namespace Afina {
namespace Backend {



// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Put(const std::string &key, const std::string &value)
{
  /*
    if (_lru_index.find(std::reference_wrapper<const std::string>(key)) != _lru_index.end()) {
      lru_node *_found = _lru_index.find(std::ref<const std::string>(key)));
      if (_found == _lru_end) {
        _lru_end->value = value;
        return true;
      }
      lru_node *_cur = new lru_node;
      _cur->key = _found->key;
      _cur->value = _found->value;
      _cur->next = NULL;
      _cur->prev = _lru_end;
      _lru_end->next = _cur;
      _lru_end = &_cur;

      if (_found->prev == _lru_head) {
        _lru_head = _lru_head->next;
      } else {
        _found->prev->next = _found->next;
        _found->next->prev = _found->prev;
      }

      delete _found;
      return true;
    } else {
      if (value.size() + key.size() > _max_size) {
        return false;
      }

      while (_cur_size + key.size() + value.size() > _max_size) {
        _cur_size -= _lru_head->key.size() + _lru_head->value.size();
        _lru_index.erase(_lru_head->key);
        _lru_head = _lru_head->next;
        _lru_head->prev = NULL;
      }

      _cur_size += value.size() + key.size();
      lru_node curr;
      curr.value = value;
      curr.key = key;
      curr.prev = NULL;
      if (_lru_end) {
        _lru_end = &curr;
      }
      curr.prev = std::move(_lru_end);
      _lru_end = std::unique_ptr<lru_node>(&curr);

      _lru_index[key] = std::ref(curr);
      return true;
    }
    */

    return true;
}

/*
// See MapBasedGlobalLockImpl.h
bool SimpleLRU::PutIfAbsent(const std::string &key, const std::string &value)
{
    if (_lru_index.find(std::reference_wrapper<const std::string>(key)) != _lru_index.end()) {
      return false;
    } else {
      return Put(key, value);
    }
}

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Set(const std::string &key, const std::string &value)
{
    if (_lru_index.find(std::reference_wrapper<const std::string>(key)) == _lru_index.end()) {
      return false;
    }
    return Put(key, value);
}
*/

// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Delete(const std::string &key)
{
  /*
    if (_lru_index.find(std::reference_wrapper<const std::string>(key)) == _lru_index.end()) {
      return false;
    }

    lru_node *cur = _lru_index.find(std::ref<const std::string>(key)));
    _lru_index.erase(key);


    if (cur->prev == _lru_head) {
      _lru_head = _lru_head->next;
    } else {
      cur->prev->next = cur->next;
      cur->next->prev = cur->prev;
    }

    delete cur;
    */
    return true;

}

/*
// See MapBasedGlobalLockImpl.h
bool SimpleLRU::Get(const std::string &key, std::string &value)
{
    if (_lru_index.find(std::ref<const std::string>(key)) == _lru_index.end()) {
      return false;
    }

    lru_node *_found = _lru_index.find(std::ref<const std::string>(key)));
    value = std::move(_found->value);

    if (_found == _lru_end) {
      return true;
    }

    _found->next = NULL;
    _found->prev = std::move(_lru_end);
    _lru_end->next = std::move(_found);
    _lru_end = &_found;

    if (_found->prev == _lru_head) {
      _lru_head = std::move(_lru_head->next);
    } else {
      _found->prev->next = std::move(_found->next);
      _found->next->prev = std::move(_found->prev);
    }

    return true;
}

*/

} // namespace Backend
} // namespace Afina
