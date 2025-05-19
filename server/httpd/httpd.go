package httpd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/PlakarKorp/kloset/appcontext"
	"github.com/PlakarKorp/kloset/network"
	"github.com/PlakarKorp/kloset/repository"
	"github.com/PlakarKorp/kloset/storage"
)

var store storage.Store
var ctx *appcontext.AppContext
var lNoDelete bool

func openRepository(w http.ResponseWriter, r *http.Request) {
	var reqOpen network.ReqOpen
	if err := json.NewDecoder(r.Body).Decode(&reqOpen); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	serializedConfig, err := store.Open(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var resOpen network.ResOpen
	resOpen.Configuration = serializedConfig
	resOpen.Err = ""
	if err := json.NewEncoder(w).Encode(resOpen); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// states
func getStates(w http.ResponseWriter, r *http.Request) {
	var reqGetIndexes network.ReqGetStates
	if err := json.NewDecoder(r.Body).Decode(&reqGetIndexes); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetStates network.ResGetStates
	indexes, err := store.GetStates()
	if err != nil {
		resGetStates.Err = err.Error()
	} else {
		resGetStates.MACs = indexes
	}
	if err := json.NewEncoder(w).Encode(resGetStates); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func putState(w http.ResponseWriter, r *http.Request) {
	var reqPutState network.ReqPutState
	if err := json.NewDecoder(r.Body).Decode(&reqPutState); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resPutIndex network.ResPutState
	data := reqPutState.Data
	_, err := store.PutState(reqPutState.MAC, bytes.NewBuffer(data))
	if err != nil {
		resPutIndex.Err = err.Error()
	}
	if err := json.NewEncoder(w).Encode(resPutIndex); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getState(w http.ResponseWriter, r *http.Request) {
	var reqGetState network.ReqGetState
	if err := json.NewDecoder(r.Body).Decode(&reqGetState); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetState network.ResGetState
	rd, err := store.GetState(reqGetState.MAC)
	if err != nil {
		resGetState.Err = err.Error()
	} else {
		data, err := io.ReadAll(rd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resGetState.Data = data
	}
	if err := json.NewEncoder(w).Encode(resGetState); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func deleteState(w http.ResponseWriter, r *http.Request) {
	if lNoDelete {
		http.Error(w, fmt.Errorf("not allowed to delete").Error(), http.StatusForbidden)
		return
	}

	var reqDeleteState network.ReqDeleteState
	if err := json.NewDecoder(r.Body).Decode(&reqDeleteState); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resDeleteState network.ResDeleteState
	err := store.DeleteState(reqDeleteState.MAC)
	if err != nil {
		resDeleteState.Err = err.Error()
	}
	if err := json.NewEncoder(w).Encode(resDeleteState); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// packfiles
func getPackfiles(w http.ResponseWriter, r *http.Request) {
	var reqGetPackfiles network.ReqGetPackfiles
	if err := json.NewDecoder(r.Body).Decode(&reqGetPackfiles); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetPackfiles network.ResGetPackfiles
	packfiles, err := store.GetPackfiles()
	if err != nil {
		resGetPackfiles.Err = err.Error()
	} else {
		resGetPackfiles.MACs = packfiles
	}
	if err := json.NewEncoder(w).Encode(resGetPackfiles); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func putPackfile(w http.ResponseWriter, r *http.Request) {
	var reqPutPackfile network.ReqPutPackfile
	if err := json.NewDecoder(r.Body).Decode(&reqPutPackfile); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resPutPackfile network.ResPutPackfile
	_, err := store.PutPackfile(reqPutPackfile.MAC, bytes.NewBuffer(reqPutPackfile.Data))
	if err != nil {
		resPutPackfile.Err = err.Error()
	}
	if err := json.NewEncoder(w).Encode(resPutPackfile); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getPackfile(w http.ResponseWriter, r *http.Request) {
	var reqGetPackfile network.ReqGetPackfile
	if err := json.NewDecoder(r.Body).Decode(&reqGetPackfile); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetPackfile network.ResGetPackfile
	rd, err := store.GetPackfile(reqGetPackfile.MAC)
	if err != nil {
		resGetPackfile.Err = err.Error()
	} else {
		data, err := io.ReadAll(rd)
		if err != nil {
			resGetPackfile.Err = err.Error()
		} else {
			resGetPackfile.Data = data
		}
	}
	if err := json.NewEncoder(w).Encode(resGetPackfile); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func GetPackfileBlob(w http.ResponseWriter, r *http.Request) {
	var reqGetPackfileBlob network.ReqGetPackfileBlob
	if err := json.NewDecoder(r.Body).Decode(&reqGetPackfileBlob); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resGetPackfileBlob network.ResGetPackfileBlob
	rd, err := store.GetPackfileBlob(reqGetPackfileBlob.MAC, reqGetPackfileBlob.Offset, reqGetPackfileBlob.Length)
	if err != nil {
		resGetPackfileBlob.Err = err.Error()
	} else {
		data, err := io.ReadAll(rd)
		if err != nil {
			resGetPackfileBlob.Err = err.Error()
		} else {
			resGetPackfileBlob.Data = data
		}
	}
	if err := json.NewEncoder(w).Encode(resGetPackfileBlob); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func deletePackfile(w http.ResponseWriter, r *http.Request) {
	if lNoDelete {
		http.Error(w, fmt.Errorf("not allowed to delete").Error(), http.StatusForbidden)
		return
	}

	var reqDeletePackfile network.ReqDeletePackfile
	if err := json.NewDecoder(r.Body).Decode(&reqDeletePackfile); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var resDeletePackfile network.ResDeletePackfile
	err := store.DeletePackfile(reqDeletePackfile.MAC)
	if err != nil {
		resDeletePackfile.Err = err.Error()
	}
	if err := json.NewEncoder(w).Encode(resDeletePackfile); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getLocks(w http.ResponseWriter, r *http.Request) {
	var req network.ReqGetLocks
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	locks, err := store.GetLocks()
	res := network.ResGetLocks{
		Locks: locks,
	}
	if err != nil {
		res.Err = err.Error()
	}
	if err := json.NewEncoder(w).Encode(&res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func putLock(w http.ResponseWriter, r *http.Request) {
	var req network.ReqPutLock
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var res network.ResPutLock
	if _, err := store.PutLock(req.Mac, bytes.NewReader(req.Data)); err != nil {
		res.Err = err.Error()
	}

	if err := json.NewEncoder(w).Encode(&res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func getLock(w http.ResponseWriter, r *http.Request) {
	var req network.ReqGetLock
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var res network.ResGetLock
	rd, err := store.GetLock(req.Mac)
	if err != nil {
		res.Err = err.Error()
	} else {
		data, err := io.ReadAll(rd)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		res.Data = data
	}

	if err := json.NewEncoder(w).Encode(&res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func deleteLock(w http.ResponseWriter, r *http.Request) {
	var req network.ReqDeleteLock
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var res network.ResDeleteLock
	if err := store.DeleteLock(req.Mac); err != nil {
		res.Err = err.Error()
	}

	if err := json.NewEncoder(w).Encode(&res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func Server(repo *repository.Repository, addr string, noDelete bool) error {
	lNoDelete = noDelete
	store = repo.Store()
	ctx = repo.AppContext()

	mux := http.NewServeMux()

	mux.HandleFunc("GET /", openRepository)

	mux.HandleFunc("GET /states", getStates)
	mux.HandleFunc("PUT /state", putState)
	mux.HandleFunc("GET /state", getState)
	mux.HandleFunc("DELETE /state", deleteState)

	mux.HandleFunc("GET /packfiles", getPackfiles)
	mux.HandleFunc("PUT /packfile", putPackfile)
	mux.HandleFunc("GET /packfile", getPackfile)
	mux.HandleFunc("GET /packfile/blob", GetPackfileBlob)
	mux.HandleFunc("DELETE /packfile", deletePackfile)

	mux.HandleFunc("GET /locks", getLocks)
	mux.HandleFunc("PUT /lock", putLock)
	mux.HandleFunc("GET /lock", getLock)
	mux.HandleFunc("DELETE /lock", deleteLock)

	server := &http.Server{Addr: addr, Handler: mux}
	go func() {
		<-repo.AppContext().Done()
		server.Shutdown(repo.AppContext().Context)
	}()

	return server.ListenAndServe()
}
