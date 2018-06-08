module.exports = async event => {
    try {
        return await event;
    }
    catch (error) {   
        if(!error.response) { return error; }
        return error.response;
    }
}
